// DO NOT EDIT. This file was generated with:
//
// $ edb gen-rust-ast

//! Abstract Syntax Tree for EdgeQL
#![allow(non_camel_case_types)]

use indexmap::IndexMap;

#[derive(Debug, Clone)]
pub enum Base {
    OptionValue(OptionValue),
    Options(Options),
    Expr(Box<Expr>),
    SortExpr(SortExpr),
    Alias(Alias),
    BaseObjectRef(BaseObjectRef),
    Index(Index),
    Slice(Slice),
    WindowSpec(WindowSpec),
    TypeExpr(TypeExpr),
    FuncParam(FuncParam),
    TypeIntersection(TypeIntersection),
    Ptr(Ptr),
    Splat(Splat),
    TupleElement(TupleElement),
    Command(Command),
    ShapeOperation(ShapeOperation),
    GroupingIdentList(GroupingIdentList),
    GroupingElement(GroupingElement),
    Transaction(Transaction),
    DDL(DDL),
    ConfigOp(ConfigOp),
    SDL(SDL),
}

#[derive(Debug, Clone)]
pub enum OptionValue {
    OptionFlag(OptionFlag),
}

#[derive(Debug, Clone)]
pub struct OptionFlag {
    pub name: String,
    pub val: bool,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub options: IndexMap<String, OptionValue>,
}

#[derive(Debug, Clone)]
pub enum Expr {
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
    OptionalExpr(OptionalExpr),
}

#[derive(Debug, Clone)]
pub struct Placeholder {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    pub path: Box<Expr>,
    pub direction: Option<SortOrder>,
    pub nones_order: Option<NonesOrder>,
}

#[derive(Debug, Clone)]
pub enum Alias {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct AliasedExpr {
    pub alias: String,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct ModuleAliasDecl {
    pub module: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum BaseObjectRef {
    ObjectRef(ObjectRef),
    PseudoObjectRef(PseudoObjectRef),
}

#[derive(Debug, Clone)]
pub struct ObjectRef {
    pub name: String,
    pub module: Option<String>,
    pub itemclass: Option<SchemaObjectClass>,
}

#[derive(Debug, Clone)]
pub struct PseudoObjectRef {
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum Anchor {
    IRAnchor(IRAnchor),
    SpecialAnchor(SpecialAnchor),
}

#[derive(Debug, Clone)]
pub struct IRAnchor {
    pub name: String,
    pub has_dml: bool,
}

#[derive(Debug, Clone)]
pub struct SpecialAnchor {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct DetachedExpr {
    pub expr: Box<Expr>,
    pub preserve_path_prefix: bool,
}

#[derive(Debug, Clone)]
pub struct GlobalExpr {
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub index: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct Slice {
    pub start: Option<Box<Expr>>,
    pub stop: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct Indirection {
    pub arg: Box<Expr>,
    pub indirection: Vec<IndirectionIndirection>,
}

#[derive(Debug, Clone)]
pub enum IndirectionIndirection {
    Index(Index),
    Slice(Slice),
}

#[derive(Debug, Clone)]
pub struct BinOp {
    pub left: Box<Expr>,
    pub op: String,
    pub right: Box<Expr>,
    pub rebalanced: bool,
    pub set_constructor: bool,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub orderby: Vec<SortExpr>,
    pub partition: Vec<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub func: FunctionCallFunc,
    pub args: Vec<Box<Expr>>,
    pub kwargs: IndexMap<String, Box<Expr>>,
    pub window: Option<WindowSpec>,
}

#[derive(Debug, Clone)]
pub enum FunctionCallFunc {
    Tuple((String, String)),
    str(String),
}

#[derive(Debug, Clone)]
pub enum BaseConstant {
    Constant(Constant),
    BytesConstant(BytesConstant),
}

#[derive(Debug, Clone)]
pub struct Constant {
    pub kind: ConstantKind,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct BytesConstant {
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct UnaryOp {
    pub op: String,
    pub operand: Box<Expr>,
}

#[derive(Debug, Clone)]
pub enum TypeExpr {
    TypeOf(TypeOf),
    TypeExprLiteral(TypeExprLiteral),
    TypeName(TypeName),
    TypeOp(TypeOp),
}

#[derive(Debug, Clone)]
pub struct TypeOf {
    pub name: Option<String>,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct TypeExprLiteral {
    pub name: Option<String>,
    pub val: Constant,
}

#[derive(Debug, Clone)]
pub struct TypeName {
    pub name: Option<String>,
    pub maintype: BaseObjectRef,
    pub subtypes: Option<Vec<TypeExpr>>,
    pub dimensions: Option<Vec<i64>>,
}

#[derive(Debug, Clone)]
pub struct TypeOp {
    pub name: Option<String>,
    pub left: Box<TypeExpr>,
    pub op: String,
    pub right: Box<TypeExpr>,
}

#[derive(Debug, Clone)]
pub struct FuncParam {
    pub name: String,
    pub r#type: TypeExpr,
    pub typemod: TypeModifier,
    pub kind: ParameterKind,
    pub default: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct IsOp {
    pub left: Box<Expr>,
    pub op: String,
    pub right: TypeExpr,
}

#[derive(Debug, Clone)]
pub struct TypeIntersection {
    pub r#type: TypeExpr,
}

#[derive(Debug, Clone)]
pub struct Ptr {
    pub name: String,
    pub direction: Option<String>,
    pub r#type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Splat {
    pub depth: i64,
    pub r#type: Option<TypeExpr>,
    pub intersection: Option<TypeIntersection>,
}

#[derive(Debug, Clone)]
pub struct Path {
    pub steps: Vec<PathSteps>,
    pub partial: bool,
}

#[derive(Debug, Clone)]
pub enum PathSteps {
    Expr(Box<Expr>),
    Ptr(Ptr),
    TypeIntersection(TypeIntersection),
    ObjectRef(ObjectRef),
    Splat(Splat),
}

#[derive(Debug, Clone)]
pub struct TypeCast {
    pub expr: Box<Expr>,
    pub r#type: TypeExpr,
    pub cardinality_mod: Option<CardinalityModifier>,
}

#[derive(Debug, Clone)]
pub struct Introspect {
    pub r#type: TypeExpr,
}

#[derive(Debug, Clone)]
pub struct IfElse {
    pub condition: Box<Expr>,
    pub if_expr: Box<Expr>,
    pub else_expr: Box<Expr>,
    pub python_style: bool,
}

#[derive(Debug, Clone)]
pub struct TupleElement {
    pub name: Ptr,
    pub val: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct NamedTuple {
    pub elements: Vec<TupleElement>,
}

#[derive(Debug, Clone)]
pub struct Tuple {
    pub elements: Vec<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct Array {
    pub elements: Vec<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct Set {
    pub elements: Vec<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub enum Command {
    SessionSetAliasDecl(SessionSetAliasDecl),
    SessionResetAliasDecl(SessionResetAliasDecl),
    SessionResetModule(SessionResetModule),
    SessionResetAllAliases(SessionResetAllAliases),
    DescribeStmt(DescribeStmt),
    ExplainStmt(ExplainStmt),
    AdministerStmt(AdministerStmt),
}

#[derive(Debug, Clone)]
pub struct SessionSetAliasDecl {
    pub aliases: Option<Vec<Alias>>,
    pub decl: ModuleAliasDecl,
}

#[derive(Debug, Clone)]
pub struct SessionResetAliasDecl {
    pub aliases: Option<Vec<Alias>>,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct SessionResetModule {
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct SessionResetAllAliases {
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct ShapeOperation {
    pub op: ShapeOp,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct Shape {
    pub expr: Option<Box<Expr>>,
    pub elements: Vec<ShapeElement>,
}

#[derive(Debug, Clone)]
pub enum Query {
    SelectQuery(SelectQuery),
    GroupQuery(GroupQuery),
    InternalGroupQuery(InternalGroupQuery),
    InsertQuery(InsertQuery),
    UpdateQuery(UpdateQuery),
    DeleteQuery(DeleteQuery),
    ForQuery(ForQuery),
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub aliases: Option<Vec<Alias>>,
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
    pub offset: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
    pub rptr_passthrough: bool,
    pub implicit: bool,
}

#[derive(Debug, Clone)]
pub struct GroupingIdentList {
    pub elements: Vec<GroupingIdentListElements>,
}

#[derive(Debug, Clone)]
pub enum GroupingIdentListElements {
    ObjectRef(ObjectRef),
    Path(Path),
    GroupingIdentList(GroupingIdentList),
}

#[derive(Debug, Clone)]
pub enum GroupingElement {
    GroupingSimple(GroupingSimple),
    GroupingSets(GroupingSets),
    GroupingOperation(GroupingOperation),
}

#[derive(Debug, Clone)]
pub struct GroupingSimple {
    pub element: GroupingSimpleElement,
}

#[derive(Debug, Clone)]
pub enum GroupingSimpleElement {
    ObjectRef(ObjectRef),
    Path(Path),
    GroupingIdentList(GroupingIdentList),
}

#[derive(Debug, Clone)]
pub struct GroupingSets {
    pub sets: Vec<GroupingElement>,
}

#[derive(Debug, Clone)]
pub struct GroupingOperation {
    pub oper: String,
    pub elements: Vec<GroupingOperationElements>,
}

#[derive(Debug, Clone)]
pub enum GroupingOperationElements {
    ObjectRef(ObjectRef),
    Path(Path),
    GroupingIdentList(GroupingIdentList),
}

#[derive(Debug, Clone)]
pub struct GroupQuery {
    pub aliases: Option<Vec<Alias>>,
    pub subject_alias: Option<String>,
    pub using: Option<Vec<AliasedExpr>>,
    pub by: Vec<GroupingElement>,
    pub subject: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct InternalGroupQuery {
    pub aliases: Option<Vec<Alias>>,
    pub subject_alias: Option<String>,
    pub using: Option<Vec<AliasedExpr>>,
    pub by: Vec<GroupingElement>,
    pub subject: Box<Expr>,
    pub group_alias: String,
    pub grouping_alias: Option<String>,
    pub from_desugaring: bool,
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
}

#[derive(Debug, Clone)]
pub struct InsertQuery {
    pub aliases: Option<Vec<Alias>>,
    pub subject: ObjectRef,
    pub shape: Vec<ShapeElement>,
    pub unless_conflict: Option<(Option<Box<Expr>>, Option<Box<Expr>>)>,
}

#[derive(Debug, Clone)]
pub struct UpdateQuery {
    pub aliases: Option<Vec<Alias>>,
    pub shape: Vec<ShapeElement>,
    pub subject: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DeleteQuery {
    pub aliases: Option<Vec<Alias>>,
    pub subject: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
    pub offset: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct ForQuery {
    pub aliases: Option<Vec<Alias>>,
    pub from_desugaring: bool,
    pub has_union: bool,
    pub optional: bool,
    pub iterator: Box<Expr>,
    pub iterator_alias: String,
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
}

#[derive(Debug, Clone)]
pub enum Transaction {
    StartTransaction(StartTransaction),
    CommitTransaction(CommitTransaction),
    RollbackTransaction(RollbackTransaction),
    DeclareSavepoint(DeclareSavepoint),
    RollbackToSavepoint(RollbackToSavepoint),
    ReleaseSavepoint(ReleaseSavepoint),
}

#[derive(Debug, Clone)]
pub struct StartTransaction {
    pub isolation: Option<TransactionIsolationLevel>,
    pub access: Option<TransactionAccessMode>,
    pub deferrable: Option<TransactionDeferMode>,
}

#[derive(Debug, Clone)]
pub struct CommitTransaction {}

#[derive(Debug, Clone)]
pub struct RollbackTransaction {}

#[derive(Debug, Clone)]
pub struct DeclareSavepoint {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct RollbackToSavepoint {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ReleaseSavepoint {
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum DDL {
    Position(Position),
    DDLOperation(DDLOperation),
    NestedQLBlock(NestedQLBlock),
    CommittedSchema(CommittedSchema),
    IndexType(IndexType),
    IndexCode(IndexCode),
    FunctionCode(FunctionCode),
    OperatorCode(OperatorCode),
    CastCode(CastCode),
}

#[derive(Debug, Clone)]
pub struct Position {
    pub r#ref: Option<ObjectRef>,
    pub position: String,
}

#[derive(Debug, Clone)]
pub enum DDLOperation {
    DDLCommand(DDLCommand),
    AlterAddInherit(AlterAddInherit),
    AlterDropInherit(AlterDropInherit),
    OnTargetDelete(OnTargetDelete),
    OnSourceDelete(OnSourceDelete),
    SetField(SetField),
    SetAccessPerms(SetAccessPerms),
}

#[derive(Debug, Clone)]
pub enum DDLCommand {
    NamedDDL(NamedDDL),
    MigrationCommand(MigrationCommand),
    FunctionCommand(FunctionCommand),
    OperatorCommand(OperatorCommand),
}

#[derive(Debug, Clone)]
pub struct AlterAddInherit {
    pub commands: Vec<DDLOperation>,
    pub position: Option<Position>,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct AlterDropInherit {
    pub commands: Vec<DDLOperation>,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct OnTargetDelete {
    pub commands: Vec<DDLOperation>,
    pub cascade: Option<LinkTargetDeleteAction>,
}

#[derive(Debug, Clone)]
pub struct OnSourceDelete {
    pub commands: Vec<DDLOperation>,
    pub cascade: Option<LinkSourceDeleteAction>,
}

#[derive(Debug, Clone)]
pub struct SetField {
    pub commands: Vec<DDLOperation>,
    pub name: String,
    pub value: SetFieldValue,
    pub special_syntax: bool,
}

#[derive(Debug, Clone)]
pub enum SetFieldValue {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct SetPointerType {
    pub commands: Vec<DDLOperation>,
    pub name: String,
    pub value: Option<TypeExpr>,
    pub special_syntax: bool,
    pub cast_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct SetPointerCardinality {
    pub commands: Vec<DDLOperation>,
    pub name: String,
    pub value: SetPointerCardinalityValue,
    pub special_syntax: bool,
    pub conv_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub enum SetPointerCardinalityValue {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct SetPointerOptionality {
    pub commands: Vec<DDLOperation>,
    pub name: String,
    pub value: SetPointerOptionalityValue,
    pub special_syntax: bool,
    pub fill_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub enum SetPointerOptionalityValue {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub enum NamedDDL {
    ObjectDDL(ObjectDDL),
    Rename(Rename),
}

#[derive(Debug, Clone)]
pub enum ObjectDDL {
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
    ConstraintCommand(ConstraintCommand),
    IndexCommand(IndexCommand),
    AccessPolicyCommand(AccessPolicyCommand),
    TriggerCommand(TriggerCommand),
    RewriteCommand(RewriteCommand),
    CastCommand(CastCommand),
}

#[derive(Debug, Clone)]
pub enum CreateObject {
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
    CreateIndexMatch(CreateIndexMatch),
}

#[derive(Debug, Clone)]
pub enum AlterObject {
    AlterMigration(AlterMigration),
    AlterDatabase(AlterDatabase),
    AlterModule(AlterModule),
    AlterRole(AlterRole),
    AlterAnnotation(AlterAnnotation),
    AlterScalarType(AlterScalarType),
    AlterProperty(AlterProperty),
    AlterConcreteUnknownPointer(AlterConcreteUnknownPointer),
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
}

#[derive(Debug, Clone)]
pub enum DropObject {
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
    DropIndexMatch(DropIndexMatch),
}

#[derive(Debug, Clone)]
pub enum CreateExtendingObject {
    CreateAnnotation(CreateAnnotation),
    CreateScalarType(CreateScalarType),
    CreateProperty(CreateProperty),
    CreateObjectType(CreateObjectType),
    CreateLink(CreateLink),
    CreateConcreteLink(CreateConcreteLink),
    CreateConstraint(CreateConstraint),
    CreateIndex(CreateIndex),
}

#[derive(Debug, Clone)]
pub struct Rename {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub new_name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct NestedQLBlock {
    pub commands: Vec<DDLOperation>,
    pub text: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MigrationCommand {
    CreateMigration(CreateMigration),
    StartMigration(StartMigration),
    AbortMigration(AbortMigration),
    PopulateMigration(PopulateMigration),
    AlterCurrentMigrationRejectProposed(AlterCurrentMigrationRejectProposed),
    DescribeCurrentMigration(DescribeCurrentMigration),
    CommitMigration(CommitMigration),
    AlterMigration(AlterMigration),
    DropMigration(DropMigration),
    ResetSchema(ResetSchema),
    StartMigrationRewrite(StartMigrationRewrite),
    AbortMigrationRewrite(AbortMigrationRewrite),
    CommitMigrationRewrite(CommitMigrationRewrite),
}

#[derive(Debug, Clone)]
pub struct CreateMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub body: NestedQLBlock,
    pub parent: Option<ObjectRef>,
    pub metadata_only: bool,
}

#[derive(Debug, Clone)]
pub struct CommittedSchema {}

#[derive(Debug, Clone)]
pub struct StartMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub target: StartMigrationTarget,
}

#[derive(Debug, Clone)]
pub enum StartMigrationTarget {
    Schema(Schema),
    CommittedSchema(CommittedSchema),
}

#[derive(Debug, Clone)]
pub struct AbortMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct PopulateMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct AlterCurrentMigrationRejectProposed {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct DescribeCurrentMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub language: DescribeLanguage,
}

#[derive(Debug, Clone)]
pub struct CommitMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct AlterMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropMigration {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct ResetSchema {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub target: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct StartMigrationRewrite {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct AbortMigrationRewrite {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub struct CommitMigrationRewrite {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
}

#[derive(Debug, Clone)]
pub enum UnqualifiedObjectCommand {
    GlobalObjectCommand(GlobalObjectCommand),
    ExtensionCommand(ExtensionCommand),
    FutureCommand(FutureCommand),
    ModuleCommand(ModuleCommand),
}

#[derive(Debug, Clone)]
pub enum GlobalObjectCommand {
    ExternalObjectCommand(ExternalObjectCommand),
    ExtensionPackageCommand(ExtensionPackageCommand),
    RoleCommand(RoleCommand),
}

#[derive(Debug, Clone)]
pub enum ExternalObjectCommand {
    DatabaseCommand(DatabaseCommand),
}

#[derive(Debug, Clone)]
pub enum DatabaseCommand {
    CreateDatabase(CreateDatabase),
    AlterDatabase(AlterDatabase),
    DropDatabase(DropDatabase),
}

#[derive(Debug, Clone)]
pub struct CreateDatabase {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub flavor: String,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub template: Option<ObjectRef>,
    pub branch_type: BranchType,
}

#[derive(Debug, Clone)]
pub struct AlterDatabase {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub flavor: String,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct DropDatabase {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub flavor: String,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub enum ExtensionPackageCommand {
    CreateExtensionPackage(CreateExtensionPackage),
    DropExtensionPackage(DropExtensionPackage),
}

#[derive(Debug, Clone)]
pub struct CreateExtensionPackage {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub version: Constant,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub body: NestedQLBlock,
}

#[derive(Debug, Clone)]
pub struct DropExtensionPackage {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub version: Constant,
}

#[derive(Debug, Clone)]
pub enum ExtensionCommand {
    CreateExtension(CreateExtension),
    DropExtension(DropExtension),
    DropFuture(DropFuture),
}

#[derive(Debug, Clone)]
pub struct CreateExtension {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub version: Option<Constant>,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropExtension {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub version: Option<Constant>,
}

#[derive(Debug, Clone)]
pub enum FutureCommand {
    CreateFuture(CreateFuture),
}

#[derive(Debug, Clone)]
pub struct CreateFuture {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropFuture {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub version: Option<Constant>,
}

#[derive(Debug, Clone)]
pub enum ModuleCommand {
    CreateModule(CreateModule),
    AlterModule(AlterModule),
    DropModule(DropModule),
}

#[derive(Debug, Clone)]
pub struct CreateModule {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterModule {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropModule {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum RoleCommand {
    CreateRole(CreateRole),
    AlterRole(AlterRole),
    DropRole(DropRole),
}

#[derive(Debug, Clone)]
pub struct CreateRole {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub superuser: bool,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct AlterRole {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropRole {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum AnnotationCommand {
    CreateAnnotation(CreateAnnotation),
    AlterAnnotation(AlterAnnotation),
    DropAnnotation(DropAnnotation),
    CreateAnnotationValue(CreateAnnotationValue),
    AlterAnnotationValue(AlterAnnotationValue),
    DropAnnotationValue(DropAnnotationValue),
}

#[derive(Debug, Clone)]
pub struct CreateAnnotation {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
    pub r#type: Option<TypeExpr>,
    pub inheritable: bool,
}

#[derive(Debug, Clone)]
pub struct AlterAnnotation {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropAnnotation {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum PseudoTypeCommand {
    CreatePseudoType(CreatePseudoType),
}

#[derive(Debug, Clone)]
pub struct CreatePseudoType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub enum ScalarTypeCommand {
    CreateScalarType(CreateScalarType),
    AlterScalarType(AlterScalarType),
    DropScalarType(DropScalarType),
}

#[derive(Debug, Clone)]
pub struct CreateScalarType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct AlterScalarType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropScalarType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum PropertyCommand {
    CreateProperty(CreateProperty),
    AlterProperty(AlterProperty),
    DropProperty(DropProperty),
    AlterConcreteUnknownPointer(AlterConcreteUnknownPointer),
    CreateConcreteProperty(CreateConcreteProperty),
    AlterConcreteProperty(AlterConcreteProperty),
    DropConcreteProperty(DropConcreteProperty),
}

#[derive(Debug, Clone)]
pub struct CreateProperty {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct AlterProperty {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropProperty {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePointer {
    CreateConcreteUnknownPointer(CreateConcreteUnknownPointer),
    CreateConcreteProperty(CreateConcreteProperty),
    CreateConcreteLink(CreateConcreteLink),
}

#[derive(Debug, Clone)]
pub struct CreateConcreteUnknownPointer {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcreteUnknownPointerTarget,
    pub cardinality: SchemaCardinality,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteUnknownPointerTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct AlterConcreteUnknownPointer {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct CreateConcreteProperty {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcretePropertyTarget,
    pub cardinality: SchemaCardinality,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertyTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct AlterConcreteProperty {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropConcreteProperty {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum ObjectTypeCommand {
    CreateObjectType(CreateObjectType),
    AlterObjectType(AlterObjectType),
    DropObjectType(DropObjectType),
}

#[derive(Debug, Clone)]
pub struct CreateObjectType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct AlterObjectType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropObjectType {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum AliasCommand {
    CreateAlias(CreateAlias),
    AlterAlias(AlterAlias),
    DropAlias(DropAlias),
}

#[derive(Debug, Clone)]
pub struct CreateAlias {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterAlias {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropAlias {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum GlobalCommand {
    CreateGlobal(CreateGlobal),
    AlterGlobal(AlterGlobal),
    DropGlobal(DropGlobal),
}

#[derive(Debug, Clone)]
pub struct CreateGlobal {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub is_required: Option<bool>,
    pub target: CreateGlobalTarget,
    pub cardinality: Option<SchemaCardinality>,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct AlterGlobal {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropGlobal {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct SetGlobalType {
    pub commands: Vec<DDLOperation>,
    pub name: String,
    pub value: Option<TypeExpr>,
    pub special_syntax: bool,
    pub cast_expr: Option<Box<Expr>>,
    pub reset_value: bool,
}

#[derive(Debug, Clone)]
pub enum LinkCommand {
    CreateLink(CreateLink),
    AlterLink(AlterLink),
    DropLink(DropLink),
    CreateConcreteLink(CreateConcreteLink),
    AlterConcreteLink(AlterConcreteLink),
    DropConcreteLink(DropConcreteLink),
}

#[derive(Debug, Clone)]
pub struct CreateLink {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct AlterLink {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropLink {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct CreateConcreteLink {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcreteLinkTarget,
    pub cardinality: SchemaCardinality,
    pub bases: Vec<TypeName>,
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct AlterConcreteLink {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropConcreteLink {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum ConstraintCommand {
    CreateConstraint(CreateConstraint),
    AlterConstraint(AlterConstraint),
    DropConstraint(DropConstraint),
    ConcreteConstraintOp(ConcreteConstraintOp),
}

#[derive(Debug, Clone)]
pub struct CreateConstraint {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
    pub subjectexpr: Option<Box<Expr>>,
    pub params: Vec<FuncParam>,
}

#[derive(Debug, Clone)]
pub struct AlterConstraint {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropConstraint {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum ConcreteConstraintOp {
    CreateConcreteConstraint(CreateConcreteConstraint),
    AlterConcreteConstraint(AlterConcreteConstraint),
    DropConcreteConstraint(DropConcreteConstraint),
}

#[derive(Debug, Clone)]
pub struct CreateConcreteConstraint {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
    pub delegated: bool,
}

#[derive(Debug, Clone)]
pub struct AlterConcreteConstraint {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropConcreteConstraint {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct IndexType {
    pub name: ObjectRef,
    pub args: Vec<Box<Expr>>,
    pub kwargs: IndexMap<String, Box<Expr>>,
}

#[derive(Debug, Clone)]
pub enum IndexCommand {
    CreateIndex(CreateIndex),
    AlterIndex(AlterIndex),
    DropIndex(DropIndex),
    ConcreteIndexCommand(ConcreteIndexCommand),
}

#[derive(Debug, Clone)]
pub struct IndexCode {
    pub language: Language,
    pub code: String,
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub r#final: bool,
    pub bases: Vec<TypeName>,
    pub kwargs: IndexMap<String, Box<Expr>>,
    pub index_types: Vec<IndexType>,
    pub code: Option<IndexCode>,
    pub params: Vec<FuncParam>,
}

#[derive(Debug, Clone)]
pub struct AlterIndex {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropIndex {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum ConcreteIndexCommand {
    CreateConcreteIndex(CreateConcreteIndex),
    AlterConcreteIndex(AlterConcreteIndex),
    DropConcreteIndex(DropConcreteIndex),
}

#[derive(Debug, Clone)]
pub struct CreateConcreteIndex {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub kwargs: IndexMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
    pub deferred: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct AlterConcreteIndex {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub kwargs: IndexMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
    pub deferred: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct DropConcreteIndex {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub kwargs: IndexMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
    pub deferred: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CreateAnnotationValue {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub value: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct AlterAnnotationValue {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub value: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropAnnotationValue {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum AccessPolicyCommand {
    CreateAccessPolicy(CreateAccessPolicy),
    AlterAccessPolicy(AlterAccessPolicy),
    DropAccessPolicy(DropAccessPolicy),
}

#[derive(Debug, Clone)]
pub struct CreateAccessPolicy {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub condition: Option<Box<Expr>>,
    pub action: AccessPolicyAction,
    pub access_kinds: Vec<AccessKind>,
    pub expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct SetAccessPerms {
    pub commands: Vec<DDLOperation>,
    pub access_kinds: Vec<AccessKind>,
    pub action: AccessPolicyAction,
}

#[derive(Debug, Clone)]
pub struct AlterAccessPolicy {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropAccessPolicy {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum TriggerCommand {
    CreateTrigger(CreateTrigger),
    AlterTrigger(AlterTrigger),
    DropTrigger(DropTrigger),
}

#[derive(Debug, Clone)]
pub struct CreateTrigger {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub timing: TriggerTiming,
    pub kinds: Vec<TriggerKind>,
    pub scope: TriggerScope,
    pub expr: Box<Expr>,
    pub condition: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct AlterTrigger {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropTrigger {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub enum RewriteCommand {
    CreateRewrite(CreateRewrite),
    AlterRewrite(AlterRewrite),
    DropRewrite(DropRewrite),
}

#[derive(Debug, Clone)]
pub struct CreateRewrite {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub kinds: Vec<RewriteKind>,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct AlterRewrite {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub kinds: Vec<RewriteKind>,
}

#[derive(Debug, Clone)]
pub struct DropRewrite {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub kinds: Vec<RewriteKind>,
}

#[derive(Debug, Clone)]
pub struct FunctionCode {
    pub language: Language,
    pub code: Option<String>,
    pub nativecode: Option<Box<Expr>>,
    pub from_function: Option<String>,
    pub from_expr: bool,
}

#[derive(Debug, Clone)]
pub enum FunctionCommand {
    CreateFunction(CreateFunction),
    AlterFunction(AlterFunction),
    DropFunction(DropFunction),
}

#[derive(Debug, Clone)]
pub struct CreateFunction {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub params: Vec<FuncParam>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub returning: TypeExpr,
    pub code: FunctionCode,
    pub nativecode: Option<Box<Expr>>,
    pub returning_typemod: TypeModifier,
}

#[derive(Debug, Clone)]
pub struct AlterFunction {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub params: Vec<FuncParam>,
    pub name: ObjectRef,
    pub code: FunctionCode,
    pub nativecode: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropFunction {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub params: Vec<FuncParam>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct OperatorCode {
    pub language: Language,
    pub from_operator: Option<Vec<String>>,
    pub from_function: Option<Vec<String>>,
    pub from_expr: bool,
    pub code: Option<String>,
}

#[derive(Debug, Clone)]
pub enum OperatorCommand {
    CreateOperator(CreateOperator),
    AlterOperator(AlterOperator),
    DropOperator(DropOperator),
}

#[derive(Debug, Clone)]
pub struct CreateOperator {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub kind: OperatorKind,
    pub params: Vec<FuncParam>,
    pub name: ObjectRef,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub returning: TypeExpr,
    pub returning_typemod: TypeModifier,
    pub code: OperatorCode,
}

#[derive(Debug, Clone)]
pub struct AlterOperator {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub kind: OperatorKind,
    pub params: Vec<FuncParam>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct DropOperator {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub kind: OperatorKind,
    pub params: Vec<FuncParam>,
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct CastCode {
    pub language: Language,
    pub from_function: String,
    pub from_expr: bool,
    pub from_cast: bool,
    pub code: String,
}

#[derive(Debug, Clone)]
pub enum CastCommand {
    CreateCast(CreateCast),
    AlterCast(AlterCast),
    DropCast(DropCast),
}

#[derive(Debug, Clone)]
pub struct CreateCast {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub from_type: TypeName,
    pub to_type: TypeName,
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
    pub code: CastCode,
    pub allow_implicit: bool,
    pub allow_assignment: bool,
}

#[derive(Debug, Clone)]
pub struct AlterCast {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub from_type: TypeName,
    pub to_type: TypeName,
}

#[derive(Debug, Clone)]
pub struct DropCast {
    pub commands: Vec<DDLOperation>,
    pub aliases: Option<Vec<Alias>>,
    pub name: ObjectRef,
    pub from_type: TypeName,
    pub to_type: TypeName,
}

#[derive(Debug, Clone)]
pub struct OptionalExpr {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub enum ConfigOp {
    ConfigSet(ConfigSet),
    ConfigInsert(ConfigInsert),
    ConfigReset(ConfigReset),
}

#[derive(Debug, Clone)]
pub struct ConfigSet {
    pub name: ObjectRef,
    pub scope: ConfigScope,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct ConfigInsert {
    pub name: ObjectRef,
    pub scope: ConfigScope,
    pub shape: Vec<ShapeElement>,
}

#[derive(Debug, Clone)]
pub struct ConfigReset {
    pub name: ObjectRef,
    pub scope: ConfigScope,
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DescribeStmt {
    pub aliases: Option<Vec<Alias>>,
    pub language: DescribeLanguage,
    pub object: DescribeStmtObject,
    pub options: Options,
}

#[derive(Debug, Clone)]
pub enum DescribeStmtObject {
    ObjectRef(ObjectRef),
    DescribeGlobal(DescribeGlobal),
}

#[derive(Debug, Clone)]
pub struct ExplainStmt {
    pub aliases: Option<Vec<Alias>>,
    pub args: Option<NamedTuple>,
    pub query: Query,
}

#[derive(Debug, Clone)]
pub struct AdministerStmt {
    pub aliases: Option<Vec<Alias>>,
    pub expr: FunctionCall,
}

#[derive(Debug, Clone)]
pub enum SDL {
    ModuleDeclaration(ModuleDeclaration),
    Schema(Schema),
}

#[derive(Debug, Clone)]
pub struct ModuleDeclaration {
    pub name: ObjectRef,
    pub declarations: Vec<ModuleDeclarationDeclarations>,
}

#[derive(Debug, Clone)]
pub enum ModuleDeclarationDeclarations {
    NamedDDL(NamedDDL),
    ModuleDeclaration(ModuleDeclaration),
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub declarations: Vec<SchemaDeclarations>,
}

#[derive(Debug, Clone)]
pub enum SchemaDeclarations {
    NamedDDL(NamedDDL),
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
pub enum ConstantKind {
    STRING,
    BOOLEAN,
    INTEGER,
    FLOAT,
    BIGINT,
    DECIMAL,
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
pub enum BranchType {
    EMPTY,
    SCHEMA,
    DATA,
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
    Modifying,
}

#[derive(Debug, Clone)]
pub enum Multiplicity {
    EMPTY,
    UNIQUE,
    DUPLICATE,
    UNKNOWN,
}

#[derive(Debug, Clone)]
pub enum IndexDeferrability {
    Prohibited,
    Permitted,
    Required,
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
    BRANCH,
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
    MULTIRANGE_TYPE,
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
