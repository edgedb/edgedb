// DO NOT EDIT. This file was generated with:
//
// $ edb gen-rust-ast

//! Abstract Syntax Tree for EdgeQL
#![allow(non_camel_case_types)]

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct OptionValue {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct OptionFlag {
    pub val: bool,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub options: HashMap<String, OptionValue>,
}

#[derive(Debug, Clone)]
pub struct Expr {
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
pub struct OptionallyAliasedExpr {
    pub alias: Option<String>,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct AliasedExpr {
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct ModuleAliasDecl {
    pub module: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BaseSessionCommand {
}

#[derive(Debug, Clone)]
pub struct BaseSessionSet {
}

#[derive(Debug, Clone)]
pub struct BaseSessionConfigSet {
    pub system: bool,
}

#[derive(Debug, Clone)]
pub struct SessionSetAliasDecl {
    pub decl: ModuleAliasDecl,
}

#[derive(Debug, Clone)]
pub struct BaseSessionReset {
}

#[derive(Debug, Clone)]
pub struct SessionResetAliasDecl {
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct SessionResetModule {
}

#[derive(Debug, Clone)]
pub struct SessionResetAllAliases {
}

#[derive(Debug, Clone)]
pub struct BaseObjectRef {
}

#[derive(Debug, Clone)]
pub struct ObjectRef {
    pub name: String,
    pub module: Option<String>,
    pub itemclass: Option<SchemaObjectClass>,
}

#[derive(Debug, Clone)]
pub struct PseudoObjectRef {
}

#[derive(Debug, Clone)]
pub struct AnyType {
}

#[derive(Debug, Clone)]
pub struct AnyTuple {
}

#[derive(Debug, Clone)]
pub struct Anchor {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct SpecialAnchor {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Source {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Subject {
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
}

#[derive(Debug, Clone)]
pub struct SetConstructorOp {
    pub op: String,
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
    pub kwargs: HashMap<String, Box<Expr>>,
    pub window: Option<WindowSpec>,
}

#[derive(Debug, Clone)]
pub enum FunctionCallFunc {
    Tuple((String, String)),
    str(String),
}

#[derive(Debug, Clone)]
pub struct BaseConstant {
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct StringConstant {
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct BaseRealConstant {
    pub is_negative: bool,
}

#[derive(Debug, Clone)]
pub struct IntegerConstant {
    pub is_negative: bool,
}

#[derive(Debug, Clone)]
pub struct FloatConstant {
    pub is_negative: bool,
}

#[derive(Debug, Clone)]
pub struct BigintConstant {
    pub is_negative: bool,
}

#[derive(Debug, Clone)]
pub struct DecimalConstant {
    pub is_negative: bool,
}

#[derive(Debug, Clone)]
pub struct BooleanConstant {
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
pub struct TypeExpr {
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TypeOf {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct TypeExprLiteral {
    pub val: StringConstant,
}

#[derive(Debug, Clone)]
pub struct TypeName {
    pub maintype: BaseObjectRef,
    pub subtypes: Option<Vec<TypeExpr>>,
    pub dimensions: Option<Vec<i64>>,
}

#[derive(Debug, Clone)]
pub struct TypeOp {
    pub left: TypeExpr,
    pub op: String,
    pub right: TypeExpr,
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
    pub ptr: ObjectRef,
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
}

#[derive(Debug, Clone)]
pub struct TupleElement {
    pub name: ObjectRef,
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
pub struct Command {
    pub aliases: Option<Vec<CommandAliases>>,
}

#[derive(Debug, Clone)]
pub enum CommandAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
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
pub struct Query {
    pub aliases: Option<Vec<QueryAliases>>,
}

#[derive(Debug, Clone)]
pub enum QueryAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct PipelinedQuery {
    pub implicit: bool,
    pub r#where: Option<Box<Expr>>,
    pub orderby: Option<Vec<SortExpr>>,
    pub offset: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
    pub rptr_passthrough: bool,
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
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
pub struct GroupingElement {
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
    pub subject_alias: Option<String>,
    pub using: Option<Vec<AliasedExpr>>,
    pub by: Vec<GroupingElement>,
    pub subject: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct InternalGroupQuery {
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
    pub subject: ObjectRef,
    pub shape: Vec<ShapeElement>,
    pub unless_conflict: Option<(Option<Box<Expr>>, Option<Box<Expr>>)>,
}

#[derive(Debug, Clone)]
pub struct UpdateQuery {
    pub shape: Vec<ShapeElement>,
    pub subject: Box<Expr>,
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DeleteQuery {
    pub subject: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct ForQuery {
    pub iterator: Box<Expr>,
    pub iterator_alias: String,
    pub result_alias: Option<String>,
    pub result: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
}

#[derive(Debug, Clone)]
pub struct StartTransaction {
    pub isolation: Option<TransactionIsolationLevel>,
    pub access: Option<TransactionAccessMode>,
    pub deferrable: Option<TransactionDeferMode>,
}

#[derive(Debug, Clone)]
pub struct CommitTransaction {
}

#[derive(Debug, Clone)]
pub struct RollbackTransaction {
}

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
pub struct DDL {
}

#[derive(Debug, Clone)]
pub struct BasesMixin {
    pub bases: Vec<TypeName>,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub r#ref: Option<ObjectRef>,
    pub position: String,
}

#[derive(Debug, Clone)]
pub struct DDLOperation {
    pub commands: Vec<DDLOperation>,
}

#[derive(Debug, Clone)]
pub struct DDLCommand {
    pub aliases: Option<Vec<DDLCommandAliases>>,
}

#[derive(Debug, Clone)]
pub enum DDLCommandAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct AlterAddInherit {
    pub position: Option<Position>,
}

#[derive(Debug, Clone)]
pub struct AlterDropInherit {
    pub commands: Vec<DDLOperation>,
}

#[derive(Debug, Clone)]
pub struct OnTargetDelete {
    pub cascade: Option<LinkTargetDeleteAction>,
}

#[derive(Debug, Clone)]
pub struct OnSourceDelete {
    pub cascade: Option<LinkSourceDeleteAction>,
}

#[derive(Debug, Clone)]
pub struct SetField {
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
    pub name: String,
    pub value: Option<TypeExpr>,
    pub special_syntax: bool,
    pub cast_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct SetPointerCardinality {
    pub name: String,
    pub special_syntax: bool,
    pub conv_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct SetPointerOptionality {
    pub name: String,
    pub special_syntax: bool,
    pub fill_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct NamedDDL {
    pub name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct ObjectDDL {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateObject {
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterObject {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropObject {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateExtendingObject {
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub struct Rename {
    pub new_name: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct NestedQLBlock {
    pub commands: Vec<DDLOperation>,
    pub text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateMigration {
    pub body: NestedQLBlock,
    pub parent: Option<ObjectRef>,
    pub metadata_only: bool,
}

#[derive(Debug, Clone)]
pub struct CommittedSchema {
}

#[derive(Debug, Clone)]
pub struct StartMigration {
    pub target: StartMigrationTarget,
}

#[derive(Debug, Clone)]
pub enum StartMigrationTarget {
    Schema(Schema),
    CommittedSchema(CommittedSchema),
}

#[derive(Debug, Clone)]
pub struct AbortMigration {
    pub aliases: Option<Vec<AbortMigrationAliases>>,
}

#[derive(Debug, Clone)]
pub enum AbortMigrationAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct PopulateMigration {
    pub aliases: Option<Vec<PopulateMigrationAliases>>,
}

#[derive(Debug, Clone)]
pub enum PopulateMigrationAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct AlterCurrentMigrationRejectProposed {
    pub aliases: Option<Vec<AlterCurrentMigrationRejectProposedAliases>>,
}

#[derive(Debug, Clone)]
pub enum AlterCurrentMigrationRejectProposedAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct DescribeCurrentMigration {
    pub language: DescribeLanguage,
}

#[derive(Debug, Clone)]
pub struct CommitMigration {
    pub aliases: Option<Vec<CommitMigrationAliases>>,
}

#[derive(Debug, Clone)]
pub enum CommitMigrationAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct AlterMigration {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropMigration {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct ResetSchema {
    pub target: ObjectRef,
}

#[derive(Debug, Clone)]
pub struct StartMigrationRewrite {
    pub aliases: Option<Vec<StartMigrationRewriteAliases>>,
}

#[derive(Debug, Clone)]
pub enum StartMigrationRewriteAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct AbortMigrationRewrite {
    pub aliases: Option<Vec<AbortMigrationRewriteAliases>>,
}

#[derive(Debug, Clone)]
pub enum AbortMigrationRewriteAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct CommitMigrationRewrite {
    pub aliases: Option<Vec<CommitMigrationRewriteAliases>>,
}

#[derive(Debug, Clone)]
pub enum CommitMigrationRewriteAliases {
    AliasedExpr(AliasedExpr),
    ModuleAliasDecl(ModuleAliasDecl),
}

#[derive(Debug, Clone)]
pub struct UnqualifiedObjectCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct GlobalObjectCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct ExternalObjectCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DatabaseCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateDatabase {
    pub template: Option<ObjectRef>,
}

#[derive(Debug, Clone)]
pub struct AlterDatabase {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropDatabase {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct ExtensionPackageCommand {
    pub object_class: SchemaObjectClass,
    pub version: StringConstant,
}

#[derive(Debug, Clone)]
pub struct CreateExtensionPackage {
    pub body: NestedQLBlock,
}

#[derive(Debug, Clone)]
pub struct DropExtensionPackage {
    pub object_class: SchemaObjectClass,
    pub version: StringConstant,
}

#[derive(Debug, Clone)]
pub struct ExtensionCommand {
    pub object_class: SchemaObjectClass,
    pub version: Option<StringConstant>,
}

#[derive(Debug, Clone)]
pub struct CreateExtension {
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropExtension {
    pub object_class: SchemaObjectClass,
    pub version: Option<StringConstant>,
}

#[derive(Debug, Clone)]
pub struct FutureCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateFuture {
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropFuture {
    pub object_class: SchemaObjectClass,
    pub version: Option<StringConstant>,
}

#[derive(Debug, Clone)]
pub struct ModuleCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateModule {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct AlterModule {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropModule {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct RoleCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateRole {
    pub superuser: bool,
}

#[derive(Debug, Clone)]
pub struct AlterRole {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropRole {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct AnnotationCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateAnnotation {
    pub r#type: Option<TypeExpr>,
    pub inheritable: bool,
}

#[derive(Debug, Clone)]
pub struct AlterAnnotation {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropAnnotation {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct PseudoTypeCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreatePseudoType {
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ScalarTypeCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateScalarType {
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub struct AlterScalarType {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropScalarType {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct PropertyCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateProperty {
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub struct AlterProperty {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropProperty {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateConcretePointer {
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcretePointerTarget,
    pub cardinality: SchemaCardinality,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePointerTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct CreateConcreteUnknownPointer {
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcreteUnknownPointerTarget,
    pub cardinality: SchemaCardinality,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteUnknownPointerTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct CreateConcreteProperty {
    pub is_required: Option<bool>,
    pub declared_overloaded: bool,
    pub target: CreateConcretePropertyTarget,
    pub cardinality: SchemaCardinality,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertyTarget {
    Expr(Box<Expr>),
    TypeExpr(TypeExpr),
    NoneType(()),
}

#[derive(Debug, Clone)]
pub struct AlterConcreteProperty {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropConcreteProperty {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct ObjectTypeCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateObjectType {
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub struct AlterObjectType {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropObjectType {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct AliasCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateAlias {
    pub r#abstract: bool,
    pub sdl_alter_if_exists: bool,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterAlias {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropAlias {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct GlobalCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateGlobal {
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
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropGlobal {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct SetGlobalType {
    pub name: String,
    pub value: Option<TypeExpr>,
    pub special_syntax: bool,
    pub cast_expr: Option<Box<Expr>>,
    pub reset_value: bool,
}

#[derive(Debug, Clone)]
pub struct LinkCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateLink {
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub struct AlterLink {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropLink {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateConcreteLink {
    pub r#final: bool,
}

#[derive(Debug, Clone)]
pub struct AlterConcreteLink {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropConcreteLink {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CallableObjectCommand {
    pub params: Vec<FuncParam>,
}

#[derive(Debug, Clone)]
pub struct ConstraintCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateConstraint {
    pub r#abstract: bool,
    pub subjectexpr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct AlterConstraint {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropConstraint {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct ConcreteConstraintOp {
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct CreateConcreteConstraint {
    pub delegated: bool,
}

#[derive(Debug, Clone)]
pub struct AlterConcreteConstraint {
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropConcreteConstraint {
    pub args: Vec<Box<Expr>>,
    pub subjectexpr: Option<Box<Expr>>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct IndexType {
    pub name: ObjectRef,
    pub args: Vec<Box<Expr>>,
    pub kwargs: HashMap<String, Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct IndexCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct IndexCode {
    pub language: Language,
    pub code: String,
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub index_types: Vec<IndexType>,
    pub code: Option<IndexCode>,
}

#[derive(Debug, Clone)]
pub struct AlterIndex {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropIndex {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct ConcreteIndexCommand {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct CreateConcreteIndex {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct AlterConcreteIndex {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropConcreteIndex {
    pub kwargs: HashMap<String, Box<Expr>>,
    pub expr: Box<Expr>,
    pub except_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct CreateAnnotationValue {
    pub value: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct AlterAnnotationValue {
    pub value: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropAnnotationValue {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct AccessPolicyCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateAccessPolicy {
    pub condition: Option<Box<Expr>>,
    pub action: AccessPolicyAction,
    pub access_kinds: Vec<AccessKind>,
    pub expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct SetAccessPerms {
    pub access_kinds: Vec<AccessKind>,
    pub action: AccessPolicyAction,
}

#[derive(Debug, Clone)]
pub struct AlterAccessPolicy {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropAccessPolicy {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct TriggerCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateTrigger {
    pub timing: TriggerTiming,
    pub kinds: Vec<TriggerKind>,
    pub scope: TriggerScope,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct AlterTrigger {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct DropTrigger {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct RewriteCommand {
    pub object_class: SchemaObjectClass,
    pub kinds: Vec<RewriteKind>,
}

#[derive(Debug, Clone)]
pub struct CreateRewrite {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct AlterRewrite {
    pub object_class: SchemaObjectClass,
    pub kinds: Vec<RewriteKind>,
}

#[derive(Debug, Clone)]
pub struct DropRewrite {
    pub object_class: SchemaObjectClass,
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
pub struct FunctionCommand {
    pub object_class: SchemaObjectClass,
}

#[derive(Debug, Clone)]
pub struct CreateFunction {
    pub returning: TypeExpr,
    pub code: FunctionCode,
    pub nativecode: Option<Box<Expr>>,
    pub returning_typemod: TypeModifier,
}

#[derive(Debug, Clone)]
pub struct AlterFunction {
    pub code: FunctionCode,
    pub nativecode: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DropFunction {
    pub object_class: SchemaObjectClass,
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
pub struct OperatorCommand {
    pub object_class: SchemaObjectClass,
    pub kind: OperatorKind,
}

#[derive(Debug, Clone)]
pub struct CreateOperator {
    pub returning: TypeExpr,
    pub returning_typemod: TypeModifier,
    pub code: OperatorCode,
}

#[derive(Debug, Clone)]
pub struct AlterOperator {
    pub object_class: SchemaObjectClass,
    pub kind: OperatorKind,
}

#[derive(Debug, Clone)]
pub struct DropOperator {
    pub object_class: SchemaObjectClass,
    pub kind: OperatorKind,
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
pub struct CastCommand {
    pub object_class: SchemaObjectClass,
    pub from_type: TypeName,
    pub to_type: TypeName,
}

#[derive(Debug, Clone)]
pub struct CreateCast {
    pub code: CastCode,
    pub allow_implicit: bool,
    pub allow_assignment: bool,
}

#[derive(Debug, Clone)]
pub struct AlterCast {
    pub object_class: SchemaObjectClass,
    pub from_type: TypeName,
    pub to_type: TypeName,
}

#[derive(Debug, Clone)]
pub struct DropCast {
    pub object_class: SchemaObjectClass,
    pub from_type: TypeName,
    pub to_type: TypeName,
}

#[derive(Debug, Clone)]
pub struct _Optional {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct ConfigOp {
    pub name: ObjectRef,
    pub scope: ConfigScope,
}

#[derive(Debug, Clone)]
pub struct ConfigSet {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct ConfigInsert {
    pub shape: Vec<ShapeElement>,
}

#[derive(Debug, Clone)]
pub struct ConfigReset {
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DescribeStmt {
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
    pub args: Option<NamedTuple>,
    pub query: Query,
}

#[derive(Debug, Clone)]
pub struct AdministerStmt {
    pub expr: FunctionCall,
}

#[derive(Debug, Clone)]
pub struct SDL {
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
