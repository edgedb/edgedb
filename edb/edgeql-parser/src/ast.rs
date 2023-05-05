// DO NOT EDIT. This file was generated with:
//
// $ edb gen-rust-ast

//! Abstract Syntax Tree for EdgeQL
#![allow(non_camel_case_types)]

use std::collections::HashMap;

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct OptionValue {
    pub name: String,
    pub kind: OptionValueKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum OptionValueKind {
    OptionFlag(OptionFlag),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct OptionFlag {
    pub val: bool,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Options {
    pub options: HashMap<String, OptionValue>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Expr {
    pub kind: ExprKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ExprKind {
    Placeholder(Placeholder),
    Anchor(Anchor),
    DetachedExpr(DetachedExpr),
    GlobalExpr(GlobalExpr),
    Indirection(Indirection),
    BinOp(BinOp),
    FunctionCall(FunctionCall),
    BaseConstant(BaseConstant),
    Parameter(Parameter),
    UnaryOp(UnaryOp),
    IsOp(IsOp),
    Path(Path),
    TypeCast(TypeCast),
    Introspect(Introspect),
    IfElse(IfElse),
    NamedTuple(NamedTuple),
    Tuple(Tuple),
    Array(Array),
    Set(Set),
    ShapeElement(ShapeElement),
    Shape(Shape),
    Query(Query),
    ConfigOp(ConfigOp),
}

#[derive(Debug, Clone, IntoPython)]
pub struct Placeholder {
    pub name: String,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct SortExpr {
    pub path: Box<Expr>,
    pub direction: Option<SortOrder>,
    pub nones_order: Option<NonesOrder>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct AliasedExpr {
    pub alias: String,
    pub expr: Box<Expr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct ModuleAliasDecl {
    pub module: String,
    pub alias: Option<String>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct BaseObjectRef {
    pub kind: BaseObjectRefKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum BaseObjectRefKind {
    ObjectRef(ObjectRef),
    PseudoObjectRef(PseudoObjectRef),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ObjectRef {
    pub name: String,
    pub module: Option<String>,
    pub itemclass: Option<SchemaObjectClass>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct PseudoObjectRef {
    pub kind: PseudoObjectRefKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum PseudoObjectRefKind {
    AnyType(AnyType),
    AnyTuple(AnyTuple),
}

#[derive(Debug, Clone, IntoPython)]
pub struct AnyType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AnyTuple {
}

#[derive(Debug, Clone, IntoPython)]
pub struct Anchor {
    pub name: String,
    pub kind: AnchorKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum AnchorKind {
    SpecialAnchor(SpecialAnchor),
}

#[derive(Debug, Clone, IntoPython)]
pub struct SpecialAnchor {
    pub kind: SpecialAnchorKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum SpecialAnchorKind {
    Source(Source),
    Subject(Subject),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Source {
    pub name: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Subject {
    pub name: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DetachedExpr {
    pub expr: Box<Expr>,
    pub preserve_path_prefix: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct GlobalExpr {
    pub name: ObjectRef,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Index {
    pub index: Box<Expr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Slice {
    pub start: Option<Box<Expr>>,
    pub stop: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Indirection {
    pub arg: Box<Expr>,
    pub indirection: Vec<IndirectionIndirection>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum IndirectionIndirection {
    Index(Index),
    Slice(Slice),
}

#[derive(Debug, Clone, IntoPython)]
pub struct BinOp {
    pub left: Box<Expr>,
    pub op: String,
    pub right: Box<Expr>,
    pub rebalanced: bool,
    pub kind: BinOpKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum BinOpKind {
    SetConstructorOp(SetConstructorOp),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetConstructorOp {
    pub op: String,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct WindowSpec {
    pub orderby: Vec<SortExpr>,
    pub partition: Vec<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct FunctionCall {
    pub func: FunctionCallFunc,
    pub args: Vec<Box<Expr>>,
    pub kwargs: HashMap<String, Box<Expr>>,
    pub window: Option<WindowSpec>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum FunctionCallFunc {
    Tuple((String, String)),
    str(String),
}

#[derive(Debug, Clone, IntoPython)]
pub struct BaseConstant {
    pub value: String,
    pub kind: BaseConstantKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum BaseConstantKind {
    StringConstant(StringConstant),
    BaseRealConstant(BaseRealConstant),
    BooleanConstant(BooleanConstant),
    BytesConstant(BytesConstant),
}

#[derive(Debug, Clone, IntoPython)]
pub struct StringConstant {
}

#[derive(Debug, Clone, IntoPython)]
pub struct BaseRealConstant {
    pub is_negative: bool,
    pub kind: BaseRealConstantKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum BaseRealConstantKind {
    IntegerConstant(IntegerConstant),
    FloatConstant(FloatConstant),
    BigintConstant(BigintConstant),
    DecimalConstant(DecimalConstant),
}

#[derive(Debug, Clone, IntoPython)]
pub struct IntegerConstant {
}

#[derive(Debug, Clone, IntoPython)]
pub struct FloatConstant {
}

#[derive(Debug, Clone, IntoPython)]
pub struct BigintConstant {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DecimalConstant {
}

#[derive(Debug, Clone, IntoPython)]
pub struct BooleanConstant {
}

#[derive(Debug, Clone, IntoPython)]
pub struct BytesConstant {
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Parameter {
    pub name: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct UnaryOp {
    pub op: String,
    pub operand: Box<Expr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct TypeExpr {
    pub name: Option<String>,
    pub kind: TypeExprKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum TypeExprKind {
    TypeOf(TypeOf),
    TypeExprLiteral(TypeExprLiteral),
    TypeName(TypeName),
    TypeOp(TypeOp),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct TypeOf {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct TypeExprLiteral {
    pub val: BaseConstant,
}

#[derive(Debug, Clone, IntoPython)]
pub struct TypeName {
    pub maintype: BaseObjectRef,
    pub subtypes: Option<Vec<TypeExpr>>,
    pub dimensions: Option<Vec<i64>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct TypeOp {
    pub left: Box<TypeExpr>,
    pub op: String,
    pub right: Box<TypeExpr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct FuncParam {
    pub name: String,
    pub r#type: TypeExpr,
    pub typemod: TypeModifier,
    pub kind: ParameterKind,
    pub default: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct IsOp {
    pub left: Box<Expr>,
    pub op: String,
    pub right: TypeExpr,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct TypeIntersection {
    pub r#type: TypeExpr,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Ptr {
    pub ptr: ObjectRef,
    pub direction: Option<String>,
    pub r#type: Option<String>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Splat {
    pub depth: i64,
    pub r#type: Option<TypeExpr>,
    pub intersection: Option<TypeIntersection>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Path {
    pub steps: Vec<PathSteps>,
    pub partial: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub enum PathSteps {
    Expr(Box<Expr>),
    Ptr(Ptr),
    TypeIntersection(TypeIntersection),
    ObjectRef(ObjectRef),
    Splat(Splat),
}

#[derive(Debug, Clone, IntoPython)]
pub struct TypeCast {
    pub expr: Box<Expr>,
    pub r#type: TypeExpr,
    pub cardinality_mod: Option<CardinalityModifier>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Introspect {
    pub r#type: TypeExpr,
}

#[derive(Debug, Clone, IntoPython)]
pub struct IfElse {
    pub condition: Box<Expr>,
    pub if_expr: Box<Expr>,
    pub else_expr: Box<Expr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct TupleElement {
    pub name: ObjectRef,
    pub val: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct NamedTuple {
    pub elements: Vec<TupleElement>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Tuple {
    pub elements: Vec<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Array {
    pub elements: Vec<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Set {
    pub elements: Vec<Box<Expr>>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Command {
    pub aliases: Option<Vec<CommandAliases>>,
    pub kind: CommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CommandAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone, IntoPython)]
pub enum CommandKind {
    SessionSetAliasDecl(SessionSetAliasDecl),
    SessionResetAliasDecl(SessionResetAliasDecl),
    SessionResetModule(SessionResetModule),
    SessionResetAllAliases(SessionResetAllAliases),
    DDLCommand(DDLCommand),
    DescribeStmt(DescribeStmt),
    ExplainStmt(ExplainStmt),
    AdministerStmt(AdministerStmt),
}

#[derive(Debug, Clone, IntoPython)]
pub struct SessionSetAliasDecl {
    pub decl: ModuleAliasDecl,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SessionResetAliasDecl {
    pub alias: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SessionResetModule {
}

#[derive(Debug, Clone, IntoPython)]
pub struct SessionResetAllAliases {
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct ShapeOperation {
    pub op: ShapeOp,
}

#[derive(Debug, Clone, IntoPython)]
pub struct ShapeElement {
    pub expr: Path,
    pub elements: Option<Vec<ShapeElement>>,
    pub compexpr: Option<Box<Expr>>,
    pub cardinality: Option<SchemaCardinality>,
    pub required: Option<bool>,
    pub operation: ShapeOperation,
    pub origin: ShapeOrigin,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
    pub offset: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Shape {
    pub expr: Option<Box<Expr>>,
    pub elements: Vec<ShapeElement>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Query {
    pub aliases: Option<Vec<QueryAliases>>,
    pub kind: QueryKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum QueryAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone, IntoPython)]
pub enum QueryKind {
    PipelinedQuery(PipelinedQuery),
    GroupQuery(GroupQuery),
    InsertQuery(InsertQuery),
    UpdateQuery(UpdateQuery),
    ForQuery(ForQuery),
}

#[derive(Debug, Clone, IntoPython)]
pub struct PipelinedQuery {
    pub implicit: bool,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
    pub offset: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
    pub rptr_passthrough: bool,
    pub kind: PipelinedQueryKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum PipelinedQueryKind {
    SelectQuery(SelectQuery),
    DeleteQuery(DeleteQuery),
}

#[derive(Debug, Clone, IntoPython)]
pub struct SelectQuery {
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct GroupingIdentList {
    pub elements: Vec<GroupingIdentListElements>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GroupingIdentListElements {
    ObjectRef(ObjectRef),
    Path(Path),
    GroupingIdentList(GroupingIdentList),
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct GroupingElement {
    pub kind: GroupingElementKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GroupingElementKind {
    GroupingSimple(GroupingSimple),
    GroupingSets(GroupingSets),
    GroupingOperation(GroupingOperation),
}

#[derive(Debug, Clone, IntoPython)]
pub struct GroupingSimple {
    pub element: GroupingSimpleElement,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GroupingSimpleElement {
    ObjectRef(ObjectRef),
    Path(Path),
    GroupingIdentList(GroupingIdentList),
}

#[derive(Debug, Clone, IntoPython)]
pub struct GroupingSets {
    pub sets: Vec<GroupingElement>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct GroupingOperation {
    pub oper: String,
    pub elements: Vec<GroupingOperationElements>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GroupingOperationElements {
    ObjectRef(ObjectRef),
    Path(Path),
    GroupingIdentList(GroupingIdentList),
}

#[derive(Debug, Clone, IntoPython)]
pub struct GroupQuery {
    pub subject_alias: Option<String>,
    pub using: Option<Vec<AliasedExpr>>,
    pub by: Vec<GroupingElement>,
    pub subject: Box<Expr>,
    pub kind: GroupQueryKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GroupQueryKind {
    InternalGroupQuery(InternalGroupQuery),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct InternalGroupQuery {
    pub group_alias: String,
    pub grouping_alias: Option<String>,
    pub from_desugaring: bool,
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct InsertQuery {
    pub subject: ObjectRef,
    pub shape: Vec<ShapeElement>,
    pub unless_conflict: Option<(Option<Box<Expr>>, Option<Box<Expr>>)>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct UpdateQuery {
    pub shape: Vec<ShapeElement>,
    pub subject: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DeleteQuery {
    pub subject: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct ForQuery {
    pub iterator: Box<Expr>,
    pub iterator_alias: String,
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct Transaction {
    pub kind: TransactionKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum TransactionKind {
    StartTransaction(StartTransaction),
    CommitTransaction(CommitTransaction),
    RollbackTransaction(RollbackTransaction),
    DeclareSavepoint(DeclareSavepoint),
    RollbackToSavepoint(RollbackToSavepoint),
    ReleaseSavepoint(ReleaseSavepoint),
}

#[derive(Debug, Clone, IntoPython)]
pub struct StartTransaction {
    pub isolation: Option<TransactionIsolationLevel>,
    pub access: Option<TransactionAccessMode>,
    pub deferrable: Option<TransactionDeferMode>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CommitTransaction {
}

#[derive(Debug, Clone, IntoPython)]
pub struct RollbackTransaction {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DeclareSavepoint {
    pub name: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct RollbackToSavepoint {
    pub name: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct ReleaseSavepoint {
    pub name: String,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct DDL {
    pub kind: DDLKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum DDLKind {
    BasesMixin(BasesMixin),
    Position(Position),
    DDLOperation(DDLOperation),
    NestedQLBlock(NestedQLBlock),
    IndexType(IndexType),
}

#[derive(Debug, Clone, IntoPython)]
pub struct BasesMixin {
    pub bases: Vec<TypeExpr>,
    pub kind: BasesMixinKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum BasesMixinKind {
    AlterAddInherit(AlterAddInherit),
    AlterDropInherit(AlterDropInherit),
    CreateExtendingObject(CreateExtendingObject),
    CreateRole(CreateRole),
    CreateConcretePointer(CreateConcretePointer),
}

#[derive(Debug, Clone, IntoPython)]
pub struct Position {
    pub r#ref: Option<ObjectRef>,
    pub position: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DDLOperation {
    pub commands: Vec<DDLOperation>,
    pub kind: DDLOperationKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum DDLOperationKind {
    DDLCommand(DDLCommand),
    AlterAddInherit(AlterAddInherit),
    AlterDropInherit(AlterDropInherit),
    OnTargetDelete(OnTargetDelete),
    OnSourceDelete(OnSourceDelete),
    SetField(SetField),
    SetAccessPerms(SetAccessPerms),
}

#[derive(Debug, Clone, IntoPython)]
pub struct DDLCommand {
    pub kind: DDLCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum DDLCommandKind {
    NamedDDL(NamedDDL),
    StartMigration(StartMigration),
    AbortMigration(AbortMigration),
    PopulateMigration(PopulateMigration),
    AlterCurrentMigrationRejectProposed(AlterCurrentMigrationRejectProposed),
    DescribeCurrentMigration(DescribeCurrentMigration),
    CommitMigration(CommitMigration),
    ResetSchema(ResetSchema),
    StartMigrationRewrite(StartMigrationRewrite),
    AbortMigrationRewrite(AbortMigrationRewrite),
    CommitMigrationRewrite(CommitMigrationRewrite),
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterAddInherit {
    pub position: Option<Position>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterDropInherit {
}

#[derive(Debug, Clone, IntoPython)]
pub struct OnTargetDelete {
    pub cascade: Option<LinkTargetDeleteAction>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct OnSourceDelete {
    pub cascade: Option<LinkSourceDeleteAction>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetField {
    pub name: String,
    pub value: SetFieldValue,
    pub special_syntax: bool,
    pub kind: SetFieldKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum SetFieldValue {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone, IntoPython)]
pub enum SetFieldKind {
    SetPointerType(SetPointerType),
    SetPointerCardinality(SetPointerCardinality),
    SetPointerOptionality(SetPointerOptionality),
    SetGlobalType(SetGlobalType),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetPointerType {
    pub name: String,
    pub value: Option<TypeExpr>,
    pub special_syntax: bool,
    pub cast_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetPointerCardinality {
    pub name: String,
    pub special_syntax: bool,
    pub conv_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetPointerOptionality {
    pub name: String,
    pub special_syntax: bool,
    pub fill_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct NamedDDL {
    pub name: ObjectRef,
    pub kind: NamedDDLKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum NamedDDLKind {
    ObjectDDL(ObjectDDL),
    Rename(Rename),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ObjectDDL {
    pub object_class: SchemaObjectClass,
    pub kind: ObjectDDLKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ObjectDDLKind {
    CreateObject(CreateObject),
    AlterObject(AlterObject),
    DropObject(DropObject),
    UnqualifiedObjectCommand(UnqualifiedObjectCommand),
    AnnotationCommand(AnnotationCommand),
    PseudoTypeCommand(PseudoTypeCommand),
    ScalarTypeCommand(ScalarTypeCommand),
    PropertyCommand(PropertyCommand),
    ObjectTypeCommand(ObjectTypeCommand),
    AliasCommand(AliasCommand),
    GlobalCommand(GlobalCommand),
    LinkCommand(LinkCommand),
    CallableObjectCommand(CallableObjectCommand),
    ConstraintCommand(ConstraintCommand),
    IndexCommand(IndexCommand),
    AccessPolicyCommand(AccessPolicyCommand),
    TriggerCommand(TriggerCommand),
    RewriteCommand(RewriteCommand),
    CastCommand(CastCommand),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateObject {
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub kind: CreateObjectKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CreateObjectKind {
    CreateExtendingObject(CreateExtendingObject),
    CreateMigration(CreateMigration),
    CreateDatabase(CreateDatabase),
    CreateExtensionPackage(CreateExtensionPackage),
    CreateExtension(CreateExtension),
    CreateFuture(CreateFuture),
    CreateModule(CreateModule),
    CreateRole(CreateRole),
    CreatePseudoType(CreatePseudoType),
    CreateConcretePointer(CreateConcretePointer),
    CreateAlias(CreateAlias),
    CreateGlobal(CreateGlobal),
    CreateConcreteConstraint(CreateConcreteConstraint),
    CreateConcreteIndex(CreateConcreteIndex),
    CreateAnnotationValue(CreateAnnotationValue),
    CreateAccessPolicy(CreateAccessPolicy),
    CreateTrigger(CreateTrigger),
    CreateRewrite(CreateRewrite),
    CreateFunction(CreateFunction),
    CreateOperator(CreateOperator),
    CreateCast(CreateCast),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterObject {
    pub kind: AlterObjectKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum AlterObjectKind {
    AlterMigration(AlterMigration),
    AlterDatabase(AlterDatabase),
    AlterModule(AlterModule),
    AlterRole(AlterRole),
    AlterAnnotation(AlterAnnotation),
    AlterScalarType(AlterScalarType),
    AlterProperty(AlterProperty),
    AlterConcreteProperty(AlterConcreteProperty),
    AlterObjectType(AlterObjectType),
    AlterAlias(AlterAlias),
    AlterGlobal(AlterGlobal),
    AlterLink(AlterLink),
    AlterConcreteLink(AlterConcreteLink),
    AlterConstraint(AlterConstraint),
    AlterConcreteConstraint(AlterConcreteConstraint),
    AlterIndex(AlterIndex),
    AlterConcreteIndex(AlterConcreteIndex),
    AlterAnnotationValue(AlterAnnotationValue),
    AlterAccessPolicy(AlterAccessPolicy),
    AlterTrigger(AlterTrigger),
    AlterRewrite(AlterRewrite),
    AlterFunction(AlterFunction),
    AlterOperator(AlterOperator),
    AlterCast(AlterCast),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropObject {
    pub kind: DropObjectKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum DropObjectKind {
    DropMigration(DropMigration),
    DropDatabase(DropDatabase),
    DropExtensionPackage(DropExtensionPackage),
    DropExtension(DropExtension),
    DropFuture(DropFuture),
    DropModule(DropModule),
    DropRole(DropRole),
    DropAnnotation(DropAnnotation),
    DropScalarType(DropScalarType),
    DropProperty(DropProperty),
    DropConcreteProperty(DropConcreteProperty),
    DropObjectType(DropObjectType),
    DropAlias(DropAlias),
    DropGlobal(DropGlobal),
    DropLink(DropLink),
    DropConcreteLink(DropConcreteLink),
    DropConstraint(DropConstraint),
    DropConcreteConstraint(DropConcreteConstraint),
    DropIndex(DropIndex),
    DropConcreteIndex(DropConcreteIndex),
    DropAnnotationValue(DropAnnotationValue),
    DropAccessPolicy(DropAccessPolicy),
    DropTrigger(DropTrigger),
    DropRewrite(DropRewrite),
    DropFunction(DropFunction),
    DropOperator(DropOperator),
    DropCast(DropCast),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateExtendingObject {
    pub r#final: bool,
    pub kind: CreateExtendingObjectKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CreateExtendingObjectKind {
    CreateAnnotation(CreateAnnotation),
    CreateScalarType(CreateScalarType),
    CreateProperty(CreateProperty),
    CreateObjectType(CreateObjectType),
    CreateLink(CreateLink),
    CreateConcreteLink(CreateConcreteLink),
    CreateConstraint(CreateConstraint),
    CreateIndex(CreateIndex),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct Rename {
    pub new_name: ObjectRef,
}

#[derive(Debug, Clone, IntoPython)]
pub struct NestedQLBlock {
    pub commands: Vec<DDLOperation>,
    pub text: Option<String>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateMigration {
    pub body: NestedQLBlock,
    pub parent: Option<ObjectRef>,
    pub metadata_only: bool,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct CommittedSchema {
}

#[derive(Debug, Clone, IntoPython)]
pub struct StartMigration {
    pub target: StartMigrationTarget,
}

#[derive(Debug, Clone, IntoPython)]
pub enum StartMigrationTarget {
    Schema(Schema),
    CommittedSchema(CommittedSchema),
}

#[derive(Debug, Clone, IntoPython)]
pub struct AbortMigration {
}

#[derive(Debug, Clone, IntoPython)]
pub struct PopulateMigration {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterCurrentMigrationRejectProposed {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DescribeCurrentMigration {
    pub language: DescribeLanguage,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CommitMigration {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterMigration {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropMigration {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ResetSchema {
    pub target: ObjectRef,
}

#[derive(Debug, Clone, IntoPython)]
pub struct StartMigrationRewrite {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AbortMigrationRewrite {
}

#[derive(Debug, Clone, IntoPython)]
pub struct CommitMigrationRewrite {
}

#[derive(Debug, Clone, IntoPython)]
pub struct UnqualifiedObjectCommand {
    pub kind: UnqualifiedObjectCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum UnqualifiedObjectCommandKind {
    GlobalObjectCommand(GlobalObjectCommand),
    ExtensionCommand(ExtensionCommand),
    FutureCommand(FutureCommand),
    ModuleCommand(ModuleCommand),
}

#[derive(Debug, Clone, IntoPython)]
pub struct GlobalObjectCommand {
    pub kind: GlobalObjectCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GlobalObjectCommandKind {
    ExternalObjectCommand(ExternalObjectCommand),
    ExtensionPackageCommand(ExtensionPackageCommand),
    RoleCommand(RoleCommand),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ExternalObjectCommand {
    pub kind: ExternalObjectCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ExternalObjectCommandKind {
    DatabaseCommand(DatabaseCommand),
}

#[derive(Debug, Clone, IntoPython)]
pub struct DatabaseCommand {
    pub object_class: SchemaObjectClass,
    pub kind: DatabaseCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum DatabaseCommandKind {
    CreateDatabase(CreateDatabase),
    AlterDatabase(AlterDatabase),
    DropDatabase(DropDatabase),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateDatabase {
    pub template: Option<ObjectRef>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterDatabase {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropDatabase {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ExtensionPackageCommand {
    pub object_class: SchemaObjectClass,
    pub version: BaseConstant,
    pub kind: ExtensionPackageCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ExtensionPackageCommandKind {
    CreateExtensionPackage(CreateExtensionPackage),
    DropExtensionPackage(DropExtensionPackage),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateExtensionPackage {
    pub body: NestedQLBlock,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropExtensionPackage {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ExtensionCommand {
    pub object_class: SchemaObjectClass,
    pub version: Option<BaseConstant>,
    pub kind: ExtensionCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ExtensionCommandKind {
    CreateExtension(CreateExtension),
    DropExtension(DropExtension),
    DropFuture(DropFuture),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateExtension {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropExtension {
}

#[derive(Debug, Clone, IntoPython)]
pub struct FutureCommand {
    pub object_class: SchemaObjectClass,
    pub kind: FutureCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum FutureCommandKind {
    CreateFuture(CreateFuture),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateFuture {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropFuture {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ModuleCommand {
    pub object_class: SchemaObjectClass,
    pub kind: ModuleCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ModuleCommandKind {
    CreateModule(CreateModule),
    AlterModule(AlterModule),
    DropModule(DropModule),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateModule {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterModule {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropModule {
}

#[derive(Debug, Clone, IntoPython)]
pub struct RoleCommand {
    pub object_class: SchemaObjectClass,
    pub kind: RoleCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum RoleCommandKind {
    CreateRole(CreateRole),
    AlterRole(AlterRole),
    DropRole(DropRole),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateRole {
    pub superuser: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterRole {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropRole {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AnnotationCommand {
    pub object_class: SchemaObjectClass,
    pub kind: AnnotationCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum AnnotationCommandKind {
    CreateAnnotation(CreateAnnotation),
    AlterAnnotation(AlterAnnotation),
    DropAnnotation(DropAnnotation),
    CreateAnnotationValue(CreateAnnotationValue),
    AlterAnnotationValue(AlterAnnotationValue),
    DropAnnotationValue(DropAnnotationValue),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateAnnotation {
    pub r#type: Option<TypeExpr>,
    pub inheritable: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterAnnotation {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropAnnotation {
}

#[derive(Debug, Clone, IntoPython)]
pub struct PseudoTypeCommand {
    pub object_class: SchemaObjectClass,
    pub kind: PseudoTypeCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum PseudoTypeCommandKind {
    CreatePseudoType(CreatePseudoType),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreatePseudoType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ScalarTypeCommand {
    pub object_class: SchemaObjectClass,
    pub kind: ScalarTypeCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ScalarTypeCommandKind {
    CreateScalarType(CreateScalarType),
    AlterScalarType(AlterScalarType),
    DropScalarType(DropScalarType),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateScalarType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterScalarType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropScalarType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct PropertyCommand {
    pub object_class: SchemaObjectClass,
    pub kind: PropertyCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum PropertyCommandKind {
    CreateProperty(CreateProperty),
    AlterProperty(AlterProperty),
    DropProperty(DropProperty),
    CreateConcreteProperty(CreateConcreteProperty),
    AlterConcreteProperty(AlterConcreteProperty),
    DropConcreteProperty(DropConcreteProperty),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateProperty {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterProperty {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropProperty {
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConcretePointer {
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcretePointerTarget,
    pub cardinality: SchemaCardinality,
    pub kind: CreateConcretePointerKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CreateConcretePointerTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone, IntoPython)]
pub enum CreateConcretePointerKind {
    CreateConcreteUnknownPointer(CreateConcreteUnknownPointer),
    CreateConcreteProperty(CreateConcreteProperty),
    CreateConcreteLink(CreateConcreteLink),
    Plain,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConcreteUnknownPointer {
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConcreteProperty {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterConcreteProperty {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropConcreteProperty {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ObjectTypeCommand {
    pub object_class: SchemaObjectClass,
    pub kind: ObjectTypeCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ObjectTypeCommandKind {
    CreateObjectType(CreateObjectType),
    AlterObjectType(AlterObjectType),
    DropObjectType(DropObjectType),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateObjectType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterObjectType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropObjectType {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AliasCommand {
    pub object_class: SchemaObjectClass,
    pub kind: AliasCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum AliasCommandKind {
    CreateAlias(CreateAlias),
    AlterAlias(AlterAlias),
    DropAlias(DropAlias),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateAlias {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterAlias {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropAlias {
}

#[derive(Debug, Clone, IntoPython)]
pub struct GlobalCommand {
    pub object_class: SchemaObjectClass,
    pub kind: GlobalCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum GlobalCommandKind {
    CreateGlobal(CreateGlobal),
    AlterGlobal(AlterGlobal),
    DropGlobal(DropGlobal),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateGlobal {
    pub is_required: Option<bool>,
    pub target: CreateGlobalTarget,
    pub cardinality: Option<SchemaCardinality>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CreateGlobalTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterGlobal {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropGlobal {
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetGlobalType {
    pub name: String,
    pub value: Option<TypeExpr>,
    pub special_syntax: bool,
    pub cast_expr: Option<Box<Expr>>,
    pub reset_value: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct LinkCommand {
    pub object_class: SchemaObjectClass,
    pub kind: LinkCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum LinkCommandKind {
    CreateLink(CreateLink),
    AlterLink(AlterLink),
    DropLink(DropLink),
    CreateConcreteLink(CreateConcreteLink),
    AlterConcreteLink(AlterConcreteLink),
    DropConcreteLink(DropConcreteLink),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateLink {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterLink {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropLink {
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConcreteLink {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterConcreteLink {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropConcreteLink {
}

#[derive(Debug, Clone, IntoPython)]
pub struct CallableObjectCommand {
    pub params: Vec<FuncParam>,
    pub kind: CallableObjectCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CallableObjectCommandKind {
    CreateConstraint(CreateConstraint),
    CreateIndex(CreateIndex),
    FunctionCommand(FunctionCommand),
    OperatorCommand(OperatorCommand),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConstraintCommand {
    pub object_class: SchemaObjectClass,
    pub kind: ConstraintCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ConstraintCommandKind {
    CreateConstraint(CreateConstraint),
    AlterConstraint(AlterConstraint),
    DropConstraint(DropConstraint),
    ConcreteConstraintOp(ConcreteConstraintOp),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConstraint {
    pub r#abstract: bool,
    pub subjectexpr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterConstraint {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropConstraint {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConcreteConstraintOp {
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
    pub kind: ConcreteConstraintOpKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ConcreteConstraintOpKind {
    CreateConcreteConstraint(CreateConcreteConstraint),
    AlterConcreteConstraint(AlterConcreteConstraint),
    DropConcreteConstraint(DropConcreteConstraint),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConcreteConstraint {
    pub delegated: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterConcreteConstraint {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropConcreteConstraint {
}

#[derive(Debug, Clone, IntoPython)]
pub struct IndexType {
    pub name: ObjectRef,
    pub args: Vec<Box<Expr>>,
    pub kwargs: HashMap<String, Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct IndexCommand {
    pub object_class: SchemaObjectClass,
    pub kind: IndexCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum IndexCommandKind {
    CreateIndex(CreateIndex),
    AlterIndex(AlterIndex),
    DropIndex(DropIndex),
    ConcreteIndexCommand(ConcreteIndexCommand),
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct IndexCode {
    pub language: Language,
    pub code: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateIndex {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub index_types: Vec<IndexType>,
    pub code: Option<IndexCode>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterIndex {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropIndex {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConcreteIndexCommand {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
    pub kind: ConcreteIndexCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ConcreteIndexCommandKind {
    CreateConcreteIndex(CreateConcreteIndex),
    AlterConcreteIndex(AlterConcreteIndex),
    DropConcreteIndex(DropConcreteIndex),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateConcreteIndex {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterConcreteIndex {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropConcreteIndex {
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateAnnotationValue {
    pub value: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterAnnotationValue {
    pub value: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropAnnotationValue {
}

#[derive(Debug, Clone, IntoPython)]
pub struct AccessPolicyCommand {
    pub object_class: SchemaObjectClass,
    pub kind: AccessPolicyCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum AccessPolicyCommandKind {
    CreateAccessPolicy(CreateAccessPolicy),
    AlterAccessPolicy(AlterAccessPolicy),
    DropAccessPolicy(DropAccessPolicy),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateAccessPolicy {
    pub condition: Option<Box<Expr>>,
    pub action: AccessPolicyAction,
    pub access_kinds: Vec<AccessKind>,
    pub expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct SetAccessPerms {
    pub access_kinds: Vec<AccessKind>,
    pub action: AccessPolicyAction,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterAccessPolicy {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropAccessPolicy {
}

#[derive(Debug, Clone, IntoPython)]
pub struct TriggerCommand {
    pub object_class: SchemaObjectClass,
    pub kind: TriggerCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum TriggerCommandKind {
    CreateTrigger(CreateTrigger),
    AlterTrigger(AlterTrigger),
    DropTrigger(DropTrigger),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateTrigger {
    pub timing: TriggerTiming,
    pub kinds: Vec<TriggerKind>,
    pub scope: TriggerScope,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterTrigger {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropTrigger {
}

#[derive(Debug, Clone, IntoPython)]
pub struct RewriteCommand {
    pub object_class: SchemaObjectClass,
    pub kinds: Vec<RewriteKind>,
    pub kind: RewriteCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum RewriteCommandKind {
    CreateRewrite(CreateRewrite),
    AlterRewrite(AlterRewrite),
    DropRewrite(DropRewrite),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateRewrite {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterRewrite {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropRewrite {
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct FunctionCode {
    pub language: Language,
    pub code: Option<String>,
    pub nativecode: Option<Box<Expr>>,
    pub from_function: Option<String>,
    pub from_expr: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct FunctionCommand {
    pub object_class: SchemaObjectClass,
    pub kind: FunctionCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum FunctionCommandKind {
    CreateFunction(CreateFunction),
    AlterFunction(AlterFunction),
    DropFunction(DropFunction),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateFunction {
    pub returning: TypeExpr,
    pub code: FunctionCode,
    pub nativecode: Option<Box<Expr>>,
    pub returning_typemod: TypeModifier,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterFunction {
    pub code: FunctionCode,
    pub nativecode: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropFunction {
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct OperatorCode {
    pub language: Language,
    pub from_operator: Option<Vec<String>>,
    pub from_function: Option<Vec<String>>,
    pub from_expr: bool,
    pub code: Option<String>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct OperatorCommand {
    pub object_class: SchemaObjectClass,
    pub kind: OperatorKind,
    pub kind1: OperatorCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum OperatorCommandKind {
    CreateOperator(CreateOperator),
    AlterOperator(AlterOperator),
    DropOperator(DropOperator),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateOperator {
    pub returning: TypeExpr,
    pub returning_typemod: TypeModifier,
    pub code: OperatorCode,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterOperator {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropOperator {
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct CastCode {
    pub language: Language,
    pub from_function: String,
    pub from_expr: bool,
    pub from_cast: bool,
    pub code: String,
}

#[derive(Debug, Clone, IntoPython)]
pub struct CastCommand {
    pub object_class: SchemaObjectClass,
    pub from_type: TypeExpr,
    pub to_type: TypeExpr,
    pub kind: CastCommandKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum CastCommandKind {
    CreateCast(CreateCast),
    AlterCast(AlterCast),
    DropCast(DropCast),
}

#[derive(Debug, Clone, IntoPython)]
pub struct CreateCast {
    pub code: CastCode,
    pub allow_implicit: bool,
    pub allow_assignment: bool,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AlterCast {
}

#[derive(Debug, Clone, IntoPython)]
pub struct DropCast {
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConfigOp {
    pub name: ObjectRef,
    pub scope: ConfigScope,
    pub kind: ConfigOpKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ConfigOpKind {
    ConfigSet(ConfigSet),
    ConfigInsert(ConfigInsert),
    ConfigReset(ConfigReset),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConfigSet {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConfigInsert {
    pub shape: Vec<ShapeElement>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct ConfigReset {
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone, IntoPython)]
pub struct DescribeStmt {
    pub language: DescribeLanguage,
    pub object: DescribeStmtObject,
    pub options: Options,
}

#[derive(Debug, Clone, IntoPython)]
pub enum DescribeStmtObject {
    ObjectRef(ObjectRef),
    DescribeGlobal(DescribeGlobal),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ExplainStmt {
    pub args: Option<NamedTuple>,
    pub query: Query,
}

#[derive(Debug, Clone, IntoPython)]
pub struct AdministerStmt {
    pub expr: FunctionCall,
}

/// Base class
#[derive(Debug, Clone, IntoPython)]
pub struct SDL {
    pub kind: SDLKind,
}

#[derive(Debug, Clone, IntoPython)]
pub enum SDLKind {
    ModuleDeclaration(ModuleDeclaration),
    Schema(Schema),
}

#[derive(Debug, Clone, IntoPython)]
pub struct ModuleDeclaration {
    pub name: ObjectRef,
    pub declarations: Vec<ModuleDeclarationDeclarations>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum ModuleDeclarationDeclarations {
    NamedDDL(DDLOperation),
    ModuleDeclaration(ModuleDeclaration),
}

#[derive(Debug, Clone, IntoPython)]
pub struct Schema {
    pub declarations: Vec<SchemaDeclarations>,
}

#[derive(Debug, Clone, IntoPython)]
pub enum SchemaDeclarations {
    NamedDDL(DDLOperation),
    ModuleDeclaration(ModuleDeclaration),
}

#[derive(Debug, Clone)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum NonesOrder {
    First,
    Last,
}

#[derive(Debug, Clone)]
pub enum CardinalityModifier {
    Optional,
    Required,
}

#[derive(Debug, Clone)]
pub enum DescribeGlobal {
    Schema,
    DatabaseConfig,
    InstanceConfig,
    Roles,
}

#[derive(Debug, Clone)]
pub enum ShapeOp {
    APPEND,
    SUBTRACT,
    ASSIGN,
    MATERIALIZE,
}

#[derive(Debug, Clone)]
pub enum ShapeOrigin {
    EXPLICIT,
    DEFAULT,
    SPLAT_EXPANSION,
    MATERIALIZATION,
}

#[derive(Debug, Clone)]
pub enum Language {
    SQL,
    EdgeQL,
}

#[derive(Debug, Clone)]
pub enum ParameterKind {
    VariadicParam,
    NamedOnlyParam,
    PositionalParam,
}

#[derive(Debug, Clone)]
pub enum TypeModifier {
    SetOfType,
    OptionalType,
    SingletonType,
}

#[derive(Debug, Clone)]
pub enum OperatorKind {
    Infix,
    Postfix,
    Prefix,
    Ternary,
}

#[derive(Debug, Clone)]
pub enum TransactionIsolationLevel {
    REPEATABLE_READ,
    SERIALIZABLE,
}

#[derive(Debug, Clone)]
pub enum TransactionAccessMode {
    READ_WRITE,
    READ_ONLY,
}

#[derive(Debug, Clone)]
pub enum TransactionDeferMode {
    DEFERRABLE,
    NOT_DEFERRABLE,
}

#[derive(Debug, Clone)]
pub enum SchemaCardinality {
    One,
    Many,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum Cardinality {
    AT_MOST_ONE,
    ONE,
    MANY,
    AT_LEAST_ONE,
    UNKNOWN,
}

#[derive(Debug, Clone)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone)]
pub enum Multiplicity {
    EMPTY,
    UNIQUE,
    DUPLICATE,
    UNKNOWN,
}

#[derive(Debug, Clone)]
pub enum AccessPolicyAction {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
pub enum AccessKind {
    Select,
    UpdateRead,
    UpdateWrite,
    Delete,
    Insert,
}

#[derive(Debug, Clone)]
pub enum TriggerTiming {
    After,
    AfterCommitOf,
}

#[derive(Debug, Clone)]
pub enum TriggerKind {
    Update,
    Delete,
    Insert,
}

#[derive(Debug, Clone)]
pub enum TriggerScope {
    Each,
    All,
}

#[derive(Debug, Clone)]
pub enum RewriteKind {
    Update,
    Insert,
}

#[derive(Debug, Clone)]
pub enum DescribeLanguage {
    DDL,
    SDL,
    TEXT,
    JSON,
}

#[derive(Debug, Clone)]
pub enum SchemaObjectClass {
    ACCESS_POLICY,
    ALIAS,
    ANNOTATION,
    ARRAY_TYPE,
    CAST,
    CONSTRAINT,
    DATABASE,
    EXTENSION,
    EXTENSION_PACKAGE,
    FUTURE,
    FUNCTION,
    GLOBAL,
    INDEX,
    LINK,
    MIGRATION,
    MODULE,
    OPERATOR,
    PARAMETER,
    PROPERTY,
    PSEUDO_TYPE,
    RANGE_TYPE,
    REWRITE,
    ROLE,
    SCALAR_TYPE,
    TRIGGER,
    TUPLE_TYPE,
    TYPE,
}

#[derive(Debug, Clone)]
pub enum LinkTargetDeleteAction {
    Restrict,
    DeleteSource,
    Allow,
    DeferredRestrict,
}

#[derive(Debug, Clone)]
pub enum LinkSourceDeleteAction {
    DeleteTarget,
    Allow,
    DeleteTargetIfOrphan,
}

#[derive(Debug, Clone)]
pub enum ConfigScope {
    INSTANCE,
    DATABASE,
    SESSION,
    GLOBAL,
}
