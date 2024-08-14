from __future__ import annotations

from typing import Any, Dict, Sequence, Optional, List, TYPE_CHECKING
import json

from dataclasses import dataclass
from ..db_interface import EdgeDatabaseStorageProviderInterface
from .. import db_interface

from ..data.data_ops import EdgeID, ObjectVal, Val, MultiSetVal, DBSchema
from ..data import data_ops as e
from ..elab_schema import add_module_from_sdl_defs

if TYPE_CHECKING:
    import sqlite3


# SQLITE_PRINT_QUERIES = True
SQLITE_PRINT_QUERIES = False


@dataclass(frozen=True)
class PropertyTypeView:
    is_primitive: bool
    is_optional: bool
    is_singular: bool
    target_type_name: List[e.QualifiedName]  # a union (choice) of names
    link_props: Dict[
        str, PropertyTypeView
    ]  # link props must be empty when is_primitive is True

    def has_lp_table(self) -> bool:
        if self.link_props:
            fetch_from_lp_table = True
        else:
            if self.is_singular:
                fetch_from_lp_table = False
            else:
                fetch_from_lp_table = True
        return fetch_from_lp_table

    def get_storage_class(self) -> str:
        if (
            self.target_type_name == [e.QualifiedName(["std", "int64"])]
            or not self.is_primitive
        ):
            return "INTEGER"
        else:
            return "TEXT"


@dataclass(frozen=True)
class TableTypeView:
    columns: Dict[str, PropertyTypeView]  # property name -> property type
    indexes: Sequence[Sequence[str]]


@dataclass(frozen=True)
class ColumnSpec:
    type: str  # "INTEGER" or "TEXT"
    is_nullable: bool


@dataclass(frozen=True)
class TableSpec:
    columns: Dict[str, ColumnSpec]
    primary_key: Sequence[str]
    indexes: Sequence[Sequence[str]]


def get_property_type_view(result_tp: e.ResultTp) -> PropertyTypeView:
    tp = result_tp.tp
    mode = result_tp.mode
    is_optional = mode.lower == e.CardNumZero
    is_singular = mode.upper == e.CardNumOne

    match tp:
        case e.NamedNominalLinkTp(name=name, linkprop=link_props):
            assert isinstance(name, e.QualifiedName)
            lp_view = {
                lpname: get_property_type_view(t)
                for (lpname, t) in link_props.val.items()
                if not isinstance(t.tp, e.ComputableTp)
            }
            return PropertyTypeView(
                is_primitive=False,
                is_optional=is_optional,
                is_singular=is_singular,
                target_type_name=[name],
                link_props=lp_view,
            )
        case e.ScalarTp(name=name):
            return PropertyTypeView(
                is_primitive=True,
                is_optional=is_optional,
                is_singular=is_singular,
                target_type_name=[name],
                link_props={},
            )
        case e.CompositeTp(kind=_, tps=_, labels=_):
            # This is extremely TODO
            return PropertyTypeView(
                is_primitive=True,
                is_optional=is_optional,
                is_singular=is_singular,
                target_type_name=[],
                link_props={},
            )
        case e.DefaultTp(expr=_, tp=inner):
            return get_property_type_view(e.ResultTp(inner, result_tp.mode))
        case e.ComputableTp(_):
            raise ValueError("Computable type is not supported yet")
        case e.UnionTp(left=l, right=r):
            l_view = get_property_type_view(e.ResultTp(l, result_tp.mode))
            r_view = get_property_type_view(e.ResultTp(r, result_tp.mode))
            assert (
                l_view.link_props == r_view.link_props
            ), "Link props mismatch"
            assert (
                l_view.is_primitive == r_view.is_primitive
            ), "Primitive mismatch"
            return PropertyTypeView(
                is_primitive=l_view.is_primitive,
                is_optional=is_optional,
                is_singular=is_singular,
                target_type_name=l_view.target_type_name
                + r_view.target_type_name,
                link_props=l_view.link_props,
            )
        case _:
            raise ValueError(f"Unimplemented type {tp}")


def get_schema_property_view(schema: e.DBSchema) -> Dict[str, TableTypeView]:
    if ("default",) not in schema.modules:
        raise ValueError("Default module not found in schema")
    default_module = schema.modules[("default",)]
    result = {}
    for name, mdef in default_module.defs.items():
        match mdef:
            case e.ModuleEntityTypeDef(
                typedef=e.ObjectTp(_),
                is_abstract=False,
                constraints=_,
                indexes=indexes,
            ):
                if isinstance(mdef.typedef, e.ScalarTp):
                    raise ValueError(
                        "User defined scalar type is not supported yet"
                    )
                type_def_view = TableTypeView(
                    columns={
                        pname: get_property_type_view(t)
                        for (pname, t) in mdef.typedef.val.items()
                        if not isinstance(t.tp, e.ComputableTp)
                    },
                    indexes=indexes,
                )
                result[name] = type_def_view
            case _:
                pass
    return result


def get_table_view_from_property_view(
    schema_property_view: Dict[str, TableTypeView]
) -> Dict[str, TableSpec]:
    result_table = {}
    for tname, tview in schema_property_view.items():
        assert tname not in result_table, "Duplicate table name"
        tdef = tview.columns

        all_single_prop_names = sorted(
            [pname for (pname, pdef) in tdef.items() if pdef.is_singular]
        )

        # add all links to indexes
        indexes = list(tview.indexes)
        for pname, pdef in tdef.items():
            if pname in all_single_prop_names and not pdef.is_primitive:
                if [pname] not in indexes:
                    indexes.append([pname])

        result_table[tname] = TableSpec(
            columns={
                "id": ColumnSpec(type="INTEGER", is_nullable=False),
                **{
                    pname: ColumnSpec(
                        type=tdef[pname].get_storage_class(),
                        is_nullable=tdef[pname].is_optional,
                    )
                    for pname in all_single_prop_names
                },
            },
            primary_key=["id"],
            indexes=indexes,
        )

        for pname, pdef in tdef.items():
            if pname in all_single_prop_names and len(pdef.link_props) == 0:
                continue

            tname_pname = f"{tname}.{pname}"
            assert tname_pname not in result_table, "Duplicate table name"

            target_type = pdef.get_storage_class()
            assert "source" not in pdef.link_props, "source is reserved"
            assert "target" not in pdef.link_props, "target is reserved"
            result_table[tname_pname] = TableSpec(
                columns={
                    "source": ColumnSpec(type="INTEGER", is_nullable=False),
                    "target": ColumnSpec(type=target_type, is_nullable=False),
                    **{
                        lpname: ColumnSpec(
                            type=pdef.link_props[lpname].get_storage_class(),
                            is_nullable=pdef.link_props[lpname].is_optional,
                        )
                        for lpname in pdef.link_props
                    },
                },
                primary_key=["source", "target"],
                indexes=[["target"], ["source", "target"]],
            )

    if (
        "objects" in result_table
        or "next_id_to_return_gen" in result_table
        or "sdl_schema" in result_table
    ):
        raise ValueError(
            "objects, next_id_to_return_gen, sdl_schema tables are reserved"
        )
    else:
        result_table["objects"] = TableSpec(
            columns={
                "id": ColumnSpec(type="INTEGER", is_nullable=False),
                "tp": ColumnSpec(type="TEXT", is_nullable=False),
            },
            primary_key=["id"],
            indexes=[],
        )

    return result_table


def convert_val_to_sqlite_val(val: e.ResultMultiSetVal) -> Any:
    def sub_convert(v: e.Val) -> Any:
        match v:
            case e.ScalarVal(tp=tp, val=v):
                match tp:
                    case e.QualifiedName(name=["std", "int64"]):
                        return v
                    case _:
                        return str(v)
            case e.RefVal(refid=refid, tpname=_, val=_):
                return refid
            case e.ArrVal(val=vals):
                return json.dumps([sub_convert(v) for v in vals])
            case _:
                raise ValueError(f"Unknown value {val}")

    assert isinstance(val, e.ResultMultiSetVal)
    if len(val.getVals()) > 1:
        raise ValueError("MultiSetVal is not supported yet")
    elif len(val.getVals()) == 0:
        return None
    else:
        return sub_convert(val.getVals()[0])


class SQLiteEdgeDatabaseStorageProvider(EdgeDatabaseStorageProviderInterface):

    def __init__(self, conn: sqlite3.Connection, schema: e.DBSchema) -> None:
        super().__init__()
        self.conn = conn
        self.schema = schema
        self.cursor = conn.cursor()
        self.should_commit_to_disk = True

        self.schema_property_view = get_schema_property_view(schema)
        self.table_view = get_table_view_from_property_view(
            self.schema_property_view
        )
        self.id_initialization()
        self.create_or_populate_schema_table()

    # result should be fetched using self.cursor.fetchall()
    def do_execute_query(self, query: str, *args):
        if SQLITE_PRINT_QUERIES:
            print(query, *args)
        self.cursor.execute(query, *args)
        if SQLITE_PRINT_QUERIES:
            print("[DONE]", query, *args)

    def get_tp_name(self, tp: e.QualifiedName) -> str:
        assert (
            len(tp.names) == 2 and tp.names[0] == "default"
        ), "Only default module is supported"
        return tp.names[1]

    def to_tp_name(self, name: str) -> e.QualifiedName:
        return e.QualifiedName(["default", name])

    def get_type_for_an_id(self, id: EdgeID) -> e.QualifiedName:
        self.do_execute_query("SELECT tp FROM objects WHERE id=?", (id,))
        tp_row = self.cursor.fetchone()
        if tp_row is None:
            raise ValueError(f"ID {id} not found in database")
        else:
            return e.QualifiedName(tp_row[0].split("::"))

    def to_type_for_an_id(self, tp: e.QualifiedName) -> str:
        return "::".join(tp.names)

    def convert_sqlite_link_props_to_object_val(
        self,
        link_props: Dict[str, Any],
        link_props_view: Dict[str, PropertyTypeView],
    ) -> ObjectVal:
        assert (
            link_props.keys() == link_props_view.keys()
        ), "Link props mismatch"
        return ObjectVal(
            {
                e.LinkPropLabel(lpname): (
                    e.Invisible(),
                    (
                        e.ResultMultiSetVal(
                            [
                                self.convert_sqlite_result_to_val(
                                    link_props[lpname],
                                    link_props_view[lpname],
                                    {},
                                )
                            ]
                        )
                        if link_props[lpname]
                        else e.ResultMultiSetVal([])
                    ),
                )
                for lpname in link_props
            }
        )

    def convert_sqlite_result_to_val(
        self,
        result_data: Any,
        result_tp: PropertyTypeView,
        link_props: Dict[str, Any],
    ) -> Val:
        if result_data is None:
            raise ValueError("Unexpected sqlite value None (Internal Error)")

        if len(link_props) > 0:

            if len(result_tp.target_type_name) == 1:
                tp_name = result_tp.target_type_name[0]
            else:
                tp_name = self.get_type_for_an_id(result_data)
            converted_link_props = (
                self.convert_sqlite_link_props_to_object_val(
                    link_props, result_tp.link_props
                )
            )
            return e.RefVal(
                refid=result_data, tpname=tp_name, val=converted_link_props
            )
        else:
            if result_tp.is_primitive:
                assert len(result_tp.target_type_name) == 1
                tp_name = result_tp.target_type_name[0]
                return e.ScalarVal(
                    tp=e.ScalarTp(name=tp_name), val=result_data
                )
            else:
                if len(result_tp.target_type_name) == 1:
                    tp_name = result_tp.target_type_name[0]
                else:
                    tp_name = self.get_type_for_an_id(result_data)
                return e.RefVal(
                    refid=result_data, tpname=tp_name, val=e.ObjectVal({})
                )

    def create_or_populate_schema_table(self) -> None:

        for tname, tspec in self.table_view.items():
            column_spec = ', '.join(
                [
                    f"{cname} {cspec.type}"
                    + ("" if cspec.is_nullable else " NOT NULL")
                    for (cname, cspec) in tspec.columns.items()
                ]
            )
            primary_key_spec = f"PRIMARY KEY ({','.join(tspec.primary_key)})"

            self.do_execute_query(
                f"CREATE TABLE IF NOT EXISTS '{tname}'"
                + f" ({column_spec}, {primary_key_spec}) STRICT, WITHOUT ROWID"
            )

            for index in tspec.indexes:
                index_name = f"{tname}_{'_'.join(index)}_idx"
                index_spec = ','.join(index)
                self.do_execute_query(
                    f"CREATE INDEX IF NOT EXISTS '{index_name}'"
                    + f" ON '{tname}' ({index_spec})"
                )

    def id_initialization(self):
        self.do_execute_query(
            "CREATE TABLE IF NOT EXISTS"
            " next_id_to_return_gen (id INTEGER PRIMARY KEY)"
        )
        self.do_execute_query("SELECT id FROM next_id_to_return_gen LIMIT 1")
        next_id_row = self.cursor.fetchone()
        if next_id_row is not None:
            self.next_id_to_return = next_id_row[0]
        else:
            self.do_execute_query(
                "INSERT INTO next_id_to_return_gen (id) VALUES (?)", (101,)
            )
        self.conn.commit()  # I am not sure whether this is needed

    def get_schema(self) -> DBSchema:
        return self.schema

    def query_ids_for_a_type(
        self, tp: e.QualifiedName, filters: e.EdgeDatabaseSelectFilter
    ) -> List[EdgeID]:

        query_args = []

        def convert_select_filter_to_condition_text(
            filter: e.EdgeDatabaseSelectFilter,
        ) -> str:
            match filter:
                case e.EdgeDatabaseEqFilter(propname=propname, arg=arg):
                    if not isinstance(arg, e.MultiSetVal):
                        raise ValueError(
                            "Only MultiSetVal is supported,"
                            " check evaluation implementation"
                        )
                    if len(arg.getVals()) != 1:
                        equivalent_disjuctive_filter = (
                            e.EdgeDatabaseDisjunctiveFilter(
                                disjuncts=[
                                    e.EdgeDatabaseEqFilter(
                                        propname=propname,
                                        arg=e.ResultMultiSetVal([arg2]),
                                    )
                                    for arg2 in arg.getVals()
                                ]
                            )
                        )
                        return convert_select_filter_to_condition_text(
                            equivalent_disjuctive_filter
                        )
                    else:
                        query_args.append(convert_val_to_sqlite_val(arg))
                        if propname == "id":
                            return "(id = ?)"
                        else:
                            this_view = self.schema_property_view[
                                tp_name
                            ].columns[propname]
                            if this_view.is_singular:
                                return f"({propname} = ?)"
                            else:
                                return (
                                    f"(EXISTS (SELECT 1 FROM '"
                                    + f"{tp_name}.{propname}"
                                    + "' WHERE source = id AND target = ?))"
                                )
                case e.EdgeDatabaseConjunctiveFilter(conjuncts=filters):
                    if len(filters) == 0:
                        return "(1=1)"
                    return (
                        "("
                        + " AND ".join(
                            [
                                convert_select_filter_to_condition_text(f)
                                for f in filters
                            ]
                        )
                        + ")"
                    )
                case e.EdgeDatabaseDisjunctiveFilter(disjuncts=filters):
                    if len(filters) == 0:
                        return "(1=0)"
                    return (
                        "("
                        + " OR ".join(
                            [
                                convert_select_filter_to_condition_text(f)
                                for f in filters
                            ]
                        )
                        + ")"
                    )
                case e.EdgeDatabaseTrueFilter():
                    return "(1=1)"
                case _:
                    raise ValueError(f"Unknown filter {filter}")

        tp_name = self.get_tp_name(tp)
        filter_clause = "WHERE " + convert_select_filter_to_condition_text(
            filters
        )

        sql_query = f"""SELECT id FROM "{tp_name}" {filter_clause} """
        self.do_execute_query(sql_query, (*query_args,))
        return [row[0] for row in self.cursor.fetchall()]

    def dump_state(self) -> object:
        self.conn.commit()

        dump_script = '\n'.join(self.conn.iterdump())
        return {
            "dump": dump_script,
        }

    def restore_state(self, dumped_state) -> None:

        # Drop all tables
        self.do_execute_query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = self.cursor.fetchall()
        drop_statements = [
            f"DROP TABLE IF EXISTS \"{table[0]}\";" for table in tables
        ]
        drop_script = '\n'.join(drop_statements)

        # Execute the drop script and the dump script
        self.conn.executescript(drop_script + '\n' + dumped_state["dump"])

    def next_id(self) -> EdgeID:
        # XXX: This is not thread safe
        self.do_execute_query("SELECT id FROM next_id_to_return_gen LIMIT 1")
        id_row = self.cursor.fetchone()
        if id_row is None:
            raise ValueError("Cannot fetch next id, check initialization")
        id = id_row[0]
        self.do_execute_query("UPDATE next_id_to_return_gen SET id = id + 1")
        return id

    def project(
        self, id: EdgeID, tp: e.QualifiedName, prop: str
    ) -> MultiSetVal:
        tp_name = self.get_tp_name(tp)
        pview = self.schema_property_view[tp_name].columns[prop]

        fetch_from_lp_table = pview.has_lp_table()

        if fetch_from_lp_table:
            lp_property_names = list(pview.link_props.keys())
            lp_table_name = f"{tp_name}.{prop}"
            if len(lp_property_names) > 0:
                query = (
                    f"SELECT target, {','.join(lp_property_names)}"
                    + f" FROM '{lp_table_name}' WHERE source=?"
                )
            else:
                query = f"SELECT target FROM '{lp_table_name}' WHERE source=?"
            self.do_execute_query(query, (id,))
            result = []
            for row in self.cursor.fetchall():
                target_id = row[0]
                lp_vals = {}
                for i, lp_prop_name in enumerate(lp_property_names):
                    lp_vals[lp_prop_name] = row[i + 1]
                result.append(
                    self.convert_sqlite_result_to_val(
                        target_id, pview, lp_vals
                    )
                )
            return e.ResultMultiSetVal(result)
        else:
            query = f"SELECT {prop} FROM {tp_name} WHERE id=?"
            self.do_execute_query(query, (id,))
            result = []
            for row in self.cursor.fetchall():
                if row[0] is not None:
                    result.append(
                        self.convert_sqlite_result_to_val(row[0], pview, {})
                    )
            return e.ResultMultiSetVal(result)

    def reverse_project(
        self, subject_ids: Sequence[EdgeID], prop: str
    ) -> MultiSetVal:
        result: List[Val] = []
        for tp_name, tdef in self.schema_property_view.items():
            for prop_name, pview in tdef.columns.items():
                if prop_name == prop:
                    if pview.has_lp_table():
                        lp_property_names = list(pview.link_props.keys())
                        lp_table_name = f"{tp_name}.{prop}"
                        if len(lp_property_names) > 0:
                            query = (
                                "SELECT source, "
                                + ','.join(lp_property_names)
                                + " FROM '"
                                + lp_table_name
                                + "' WHERE target IN ("
                                + ','.join(['?'] * len(subject_ids))
                                + ")"
                            )
                        else:
                            query = (
                                f"SELECT source FROM '{lp_table_name}'"
                                + " WHERE target IN "
                                + f"({','.join(['?'] * len(subject_ids))})"
                            )
                    else:
                        lp_property_names = []
                        query = (
                            f"SELECT id FROM '{tp_name}' WHERE {prop} IN "
                            + f"({','.join(['?'] * len(subject_ids))})"
                        )
                    self.do_execute_query(
                        query, [int(id) for id in subject_ids]
                    )
                    for row in self.cursor.fetchall():
                        source_id = row[0]
                        lp_vals = {}
                        for i, lp_prop_name in enumerate(lp_property_names):
                            lp_vals[lp_prop_name] = row[i + 1]
                        converted_lp_vals = (
                            self.convert_sqlite_link_props_to_object_val(
                                lp_vals, pview.link_props
                            )
                        )
                        result.append(
                            e.RefVal(
                                refid=source_id,
                                tpname=self.to_tp_name(tp_name),
                                val=converted_lp_vals,
                            )
                        )

        return e.ResultMultiSetVal(result)

    def insert(
        self, id: EdgeID, tp: e.QualifiedName, props: Dict[str, MultiSetVal]
    ) -> None:
        tp_name = self.get_tp_name(tp)
        tdef = self.schema_property_view[tp_name].columns

        single_props = [
            pname for (pname, pview) in tdef.items() if pview.is_singular
        ]
        single_prop_vals = [
            convert_val_to_sqlite_val(props[pname]) for pname in single_props
        ]
        self.do_execute_query(
            f"INSERT INTO {tp_name} (id, {','.join(single_props)})"
            + f" VALUES (?, {','.join(['?'] * len(single_props))})",
            (id, *single_prop_vals),
        )

        for pname, pview in tdef.items():
            if pview.has_lp_table():
                lp_table_name = f"{tp_name}.{pname}"
                lp_property_names = list(pview.link_props.keys())
                for v in props[pname].getVals():
                    if len(lp_property_names) > 0:
                        assert isinstance(v, e.RefVal)
                        lp_props = [
                            convert_val_to_sqlite_val(
                                v.val.val[e.LinkPropLabel(lp_prop_name)][1]
                            )
                            for lp_prop_name in lp_property_names
                        ]
                        self.do_execute_query(
                            "INSERT INTO '"
                            + lp_table_name
                            + "' (source, target, "
                            + ','.join(lp_property_names)
                            + f") VALUES (?, ?, "
                            + ','.join(['?'] * len(lp_property_names))
                            + ")",
                            (
                                id,
                                convert_val_to_sqlite_val(
                                    e.ResultMultiSetVal([v])
                                ),
                                *lp_props,
                            ),
                        )
                    else:
                        self.do_execute_query(
                            f"INSERT INTO '{lp_table_name}'"
                            + " (source, target) VALUES (?, ?)",
                            (
                                id,
                                convert_val_to_sqlite_val(
                                    e.ResultMultiSetVal([v])
                                ),
                            ),
                        )

        self.do_execute_query(
            f"INSERT INTO objects (id, tp) VALUES (?, ?)",
            (id, self.to_type_for_an_id(tp)),
        )

    def delete(self, id: EdgeID, tp: e.QualifiedName) -> None:
        tp_name = self.get_tp_name(tp)

        for pname, pview in self.schema_property_view[tp_name].columns.items():
            if pview.has_lp_table():
                lp_table_name = f"{tp_name}.{pname}"
                self.do_execute_query(
                    f"DELETE FROM '{lp_table_name}' WHERE source=?", (id,)
                )

        self.do_execute_query(f"DELETE FROM '{tp_name}' WHERE id=?", (id,))
        self.do_execute_query(f"DELETE FROM objects WHERE id=?", (id,))

    def update(
        self, id: EdgeID, tp: e.QualifiedName, props: Dict[str, MultiSetVal]
    ) -> None:
        tp_name = self.get_tp_name(tp)
        tdef = self.schema_property_view[tp_name]

        single_props = [
            pname
            for (pname, pview) in tdef.columns.items()
            if pview.is_singular
        ]
        single_prop_vals = [
            convert_val_to_sqlite_val(props[pname])
            for pname in single_props
            if pname in props
        ]
        if len(single_prop_vals) > 0:
            self.do_execute_query(
                f"UPDATE '{tp_name}' SET "
                f"{','.join([f'{pname}=?' for pname in props])} WHERE id=?",
                (*single_prop_vals, id),
            )

        for pname, pview in tdef.columns.items():
            if pview.has_lp_table() and pname in props:
                lp_table_name = f"{tp_name}.{pname}"
                lp_property_names = list(pview.link_props.keys())
                # Delete all existing links
                self.do_execute_query(
                    f"DELETE FROM '{lp_table_name}' WHERE source=?", (id,)
                )
                for v in props[pname].getVals():
                    assert isinstance(v, e.RefVal)
                    if len(lp_property_names) > 0:
                        lp_props = [
                            convert_val_to_sqlite_val(
                                v.val.val[e.LinkPropLabel(lp_prop_name)][1]
                            )
                            for lp_prop_name in lp_property_names
                        ]
                        self.do_execute_query(
                            "INSERT INTO '"
                            + lp_table_name
                            + "' (source, target, "
                            + ','.join(lp_property_names)
                            + ") VALUES (?, ?, "
                            + ','.join(['?'] * len(lp_property_names))
                            + ")",
                            (
                                id,
                                convert_val_to_sqlite_val(
                                    e.ResultMultiSetVal([v])
                                ),
                                *lp_props,
                            ),
                        )
                    else:
                        self.do_execute_query(
                            f"INSERT INTO '{lp_table_name}'"
                            " (source, target) VALUES (?, ?)",
                            (
                                id,
                                convert_val_to_sqlite_val(
                                    e.ResultMultiSetVal([v])
                                ),
                            ),
                        )

    # By default commit is called per query,
    # doing a bunch of consecutive queries
    # will cause significant delays.
    # This function can be used to pause the commit.
    def pause_disk_commit(self) -> None:
        self.should_commit_to_disk = False

    def resume_disk_commit(self) -> None:
        self.should_commit_to_disk = True
        self.commit()

    def commit(self) -> None:
        if self.should_commit_to_disk:
            self.conn.commit()


def schema_and_db_from_sqlite(
    sdl_file_content: Optional[str], sqlite_file_name: str
):
    import sqlite3

    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file_name)
    c = conn.cursor()

    # Check if sdl_schema table exists
    c.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='sdl_schema'"
    )
    table_exists = c.fetchone()
    db_sdl = None
    if table_exists:
        # Retrieve the pickled object from the table
        c.execute("SELECT content FROM sdl_schema LIMIT 1")
        db_sdl_row = c.fetchone()
        if db_sdl_row:
            db_sdl = db_sdl_row[0]
    else:
        # Create sdl_schema table
        c.execute("CREATE TABLE sdl_schema (content TEXT)")

    from ..new_interpreter import default_dbschema

    dbschema = default_dbschema()
    if db_sdl is None:

        if sdl_file_content is None:
            content = ""  # empty schema
        else:
            content = sdl_file_content

        # Read and store the content into sdl_schema table
        c.execute("INSERT INTO sdl_schema (content) VALUES (?)", (content,))
        dbschema = add_module_from_sdl_defs(dbschema, content)

        # Commit the changes
        conn.commit()
    else:

        sdl_content = sdl_file_content
        if sdl_content is None:
            sdl_content = ""

        # Compare content and abort if they differ
        if db_sdl != sdl_content:
            raise ValueError(
                "Passed in SDL file differs from SQLite Schema.",
                sdl_content,
                db_sdl,
            )

        dbschema = add_module_from_sdl_defs(dbschema, db_sdl)

    storage = SQLiteEdgeDatabaseStorageProvider(conn, dbschema)
    db = db_interface.EdgeDatabase(storage)
    return dbschema, db
