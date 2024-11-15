// DO NOT EDIT. This file was generated with:
//
// $ edb gen-rust-ast

//! Reductions for productions in EdgeQL
#![allow(non_camel_case_types)]

#[derive(Debug, Clone)]
pub enum Reduction {
    AbortMigrationStmt(AbortMigrationStmt),
    AccessKind(AccessKind),
    AccessKindList(AccessKindList),
    AccessPermStmt(AccessPermStmt),
    AccessPolicyAction(AccessPolicyAction),
    AccessPolicyDeclarationBlock(AccessPolicyDeclarationBlock),
    AccessPolicyDeclarationShort(AccessPolicyDeclarationShort),
    AccessUsingStmt(AccessUsingStmt),
    AccessWhenStmt(AccessWhenStmt),
    AdministerStmt(AdministerStmt),
    AliasDecl(AliasDecl),
    AliasDeclaration(AliasDeclaration),
    AliasDeclarationShort(AliasDeclarationShort),
    AliasedExpr(AliasedExpr),
    AliasedExprList(AliasedExprList),
    AliasedExprListInner(AliasedExprListInner),
    AlterAbstract(AlterAbstract),
    AlterAccessPolicyCommand(AlterAccessPolicyCommand),
    AlterAccessPolicyCommandsBlock(AlterAccessPolicyCommandsBlock),
    AlterAccessPolicyCommandsList(AlterAccessPolicyCommandsList),
    AlterAccessPolicyStmt(AlterAccessPolicyStmt),
    AlterAliasCommand(AlterAliasCommand),
    AlterAliasCommandsBlock(AlterAliasCommandsBlock),
    AlterAliasCommandsList(AlterAliasCommandsList),
    AlterAliasStmt(AlterAliasStmt),
    AlterAnnotationCommand(AlterAnnotationCommand),
    AlterAnnotationCommandsBlock(AlterAnnotationCommandsBlock),
    AlterAnnotationCommandsList(AlterAnnotationCommandsList),
    AlterAnnotationStmt(AlterAnnotationStmt),
    AlterAnnotationValueStmt(AlterAnnotationValueStmt),
    AlterBranchCommand(AlterBranchCommand),
    AlterBranchCommandsBlock(AlterBranchCommandsBlock),
    AlterBranchCommandsList(AlterBranchCommandsList),
    AlterBranchStmt(AlterBranchStmt),
    AlterCastCommand(AlterCastCommand),
    AlterCastCommandsBlock(AlterCastCommandsBlock),
    AlterCastCommandsList(AlterCastCommandsList),
    AlterCastStmt(AlterCastStmt),
    AlterCommand(AlterCommand),
    AlterCommandsBlock(AlterCommandsBlock),
    AlterCommandsList(AlterCommandsList),
    AlterConcreteConstraintCommand(AlterConcreteConstraintCommand),
    AlterConcreteConstraintCommandsBlock(AlterConcreteConstraintCommandsBlock),
    AlterConcreteConstraintCommandsList(AlterConcreteConstraintCommandsList),
    AlterConcreteConstraintStmt(AlterConcreteConstraintStmt),
    AlterConcreteIndexCommand(AlterConcreteIndexCommand),
    AlterConcreteIndexCommandsBlock(AlterConcreteIndexCommandsBlock),
    AlterConcreteIndexCommandsList(AlterConcreteIndexCommandsList),
    AlterConcreteIndexStmt(AlterConcreteIndexStmt),
    AlterConcreteLinkCommand(AlterConcreteLinkCommand),
    AlterConcreteLinkCommandsBlock(AlterConcreteLinkCommandsBlock),
    AlterConcreteLinkCommandsList(AlterConcreteLinkCommandsList),
    AlterConcreteLinkStmt(AlterConcreteLinkStmt),
    AlterConcretePropertyCommand(AlterConcretePropertyCommand),
    AlterConcretePropertyCommandsBlock(AlterConcretePropertyCommandsBlock),
    AlterConcretePropertyCommandsList(AlterConcretePropertyCommandsList),
    AlterConcretePropertyStmt(AlterConcretePropertyStmt),
    AlterConstraintStmt(AlterConstraintStmt),
    AlterCurrentMigrationStmt(AlterCurrentMigrationStmt),
    AlterDatabaseCommand(AlterDatabaseCommand),
    AlterDatabaseCommandsBlock(AlterDatabaseCommandsBlock),
    AlterDatabaseCommandsList(AlterDatabaseCommandsList),
    AlterDatabaseStmt(AlterDatabaseStmt),
    AlterDeferredStmt(AlterDeferredStmt),
    AlterExtending(AlterExtending),
    AlterFunctionCommand(AlterFunctionCommand),
    AlterFunctionCommandsBlock(AlterFunctionCommandsBlock),
    AlterFunctionCommandsList(AlterFunctionCommandsList),
    AlterFunctionStmt(AlterFunctionStmt),
    AlterGlobalCommand(AlterGlobalCommand),
    AlterGlobalCommandsBlock(AlterGlobalCommandsBlock),
    AlterGlobalCommandsList(AlterGlobalCommandsList),
    AlterGlobalStmt(AlterGlobalStmt),
    AlterIndexCommand(AlterIndexCommand),
    AlterIndexCommandsBlock(AlterIndexCommandsBlock),
    AlterIndexCommandsList(AlterIndexCommandsList),
    AlterIndexStmt(AlterIndexStmt),
    AlterLinkCommand(AlterLinkCommand),
    AlterLinkCommandsBlock(AlterLinkCommandsBlock),
    AlterLinkCommandsList(AlterLinkCommandsList),
    AlterLinkStmt(AlterLinkStmt),
    AlterMigrationCommand(AlterMigrationCommand),
    AlterMigrationCommandsBlock(AlterMigrationCommandsBlock),
    AlterMigrationCommandsList(AlterMigrationCommandsList),
    AlterMigrationStmt(AlterMigrationStmt),
    AlterModuleStmt(AlterModuleStmt),
    AlterObjectTypeCommand(AlterObjectTypeCommand),
    AlterObjectTypeCommandsBlock(AlterObjectTypeCommandsBlock),
    AlterObjectTypeCommandsList(AlterObjectTypeCommandsList),
    AlterObjectTypeStmt(AlterObjectTypeStmt),
    AlterOperatorCommand(AlterOperatorCommand),
    AlterOperatorCommandsBlock(AlterOperatorCommandsBlock),
    AlterOperatorCommandsList(AlterOperatorCommandsList),
    AlterOperatorStmt(AlterOperatorStmt),
    AlterOwnedStmt(AlterOwnedStmt),
    AlterPropertyCommand(AlterPropertyCommand),
    AlterPropertyCommandsBlock(AlterPropertyCommandsBlock),
    AlterPropertyCommandsList(AlterPropertyCommandsList),
    AlterPropertyStmt(AlterPropertyStmt),
    AlterRewriteCommand(AlterRewriteCommand),
    AlterRewriteCommandsBlock(AlterRewriteCommandsBlock),
    AlterRewriteCommandsList(AlterRewriteCommandsList),
    AlterRewriteStmt(AlterRewriteStmt),
    AlterRoleCommand(AlterRoleCommand),
    AlterRoleCommandsBlock(AlterRoleCommandsBlock),
    AlterRoleCommandsList(AlterRoleCommandsList),
    AlterRoleExtending(AlterRoleExtending),
    AlterRoleStmt(AlterRoleStmt),
    AlterScalarTypeCommand(AlterScalarTypeCommand),
    AlterScalarTypeCommandsBlock(AlterScalarTypeCommandsBlock),
    AlterScalarTypeCommandsList(AlterScalarTypeCommandsList),
    AlterScalarTypeStmt(AlterScalarTypeStmt),
    AlterSimpleExtending(AlterSimpleExtending),
    AlterTriggerCommand(AlterTriggerCommand),
    AlterTriggerCommandsBlock(AlterTriggerCommandsBlock),
    AlterTriggerCommandsList(AlterTriggerCommandsList),
    AlterTriggerStmt(AlterTriggerStmt),
    AnalyzeStmt(AnalyzeStmt),
    AnnotationDeclaration(AnnotationDeclaration),
    AnnotationDeclarationShort(AnnotationDeclarationShort),
    AnyIdentifier(AnyIdentifier),
    AnyNodeName(AnyNodeName),
    AtomicExpr(AtomicExpr),
    AtomicPath(AtomicPath),
    BaseAtomicExpr(BaseAtomicExpr),
    BaseBooleanConstant(BaseBooleanConstant),
    BaseBytesConstant(BaseBytesConstant),
    BaseName(BaseName),
    BaseNumberConstant(BaseNumberConstant),
    BaseStringConstant(BaseStringConstant),
    BranchOptions(BranchOptions),
    BranchStmt(BranchStmt),
    ByClause(ByClause),
    CastAllowedUse(CastAllowedUse),
    CastCode(CastCode),
    Collection(Collection),
    CollectionTypeName(CollectionTypeName),
    ColonedIdents(ColonedIdents),
    CommitMigrationStmt(CommitMigrationStmt),
    CompareOp(CompareOp),
    ComputableShapePointer(ComputableShapePointer),
    ConcreteConstraintBlock(ConcreteConstraintBlock),
    ConcreteConstraintShort(ConcreteConstraintShort),
    ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock),
    ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort),
    ConcreteLinkBlock(ConcreteLinkBlock),
    ConcreteLinkShort(ConcreteLinkShort),
    ConcretePropertyBlock(ConcretePropertyBlock),
    ConcretePropertyShort(ConcretePropertyShort),
    ConcreteUnknownPointerBlock(ConcreteUnknownPointerBlock),
    ConcreteUnknownPointerObjectShort(ConcreteUnknownPointerObjectShort),
    ConcreteUnknownPointerShort(ConcreteUnknownPointerShort),
    ConfigOp(ConfigOp),
    ConfigScope(ConfigScope),
    ConfigStmt(ConfigStmt),
    Constant(Constant),
    ConstraintDeclaration(ConstraintDeclaration),
    ConstraintDeclarationShort(ConstraintDeclarationShort),
    CreateAccessPolicyCommand(CreateAccessPolicyCommand),
    CreateAccessPolicyCommandsBlock(CreateAccessPolicyCommandsBlock),
    CreateAccessPolicyCommandsList(CreateAccessPolicyCommandsList),
    CreateAccessPolicySDLCommandFull(CreateAccessPolicySDLCommandFull),
    CreateAccessPolicySDLCommandShort(CreateAccessPolicySDLCommandShort),
    CreateAccessPolicySDLCommandsBlock(CreateAccessPolicySDLCommandsBlock),
    CreateAccessPolicySDLCommandsList(CreateAccessPolicySDLCommandsList),
    CreateAccessPolicyStmt(CreateAccessPolicyStmt),
    CreateAliasCommand(CreateAliasCommand),
    CreateAliasCommandsBlock(CreateAliasCommandsBlock),
    CreateAliasCommandsList(CreateAliasCommandsList),
    CreateAliasSDLCommandFull(CreateAliasSDLCommandFull),
    CreateAliasSDLCommandShort(CreateAliasSDLCommandShort),
    CreateAliasSDLCommandsBlock(CreateAliasSDLCommandsBlock),
    CreateAliasSDLCommandsList(CreateAliasSDLCommandsList),
    CreateAliasSingleSDLCommandBlock(CreateAliasSingleSDLCommandBlock),
    CreateAliasStmt(CreateAliasStmt),
    CreateAnnotationCommand(CreateAnnotationCommand),
    CreateAnnotationCommandsBlock(CreateAnnotationCommandsBlock),
    CreateAnnotationCommandsList(CreateAnnotationCommandsList),
    CreateAnnotationStmt(CreateAnnotationStmt),
    CreateAnnotationValueStmt(CreateAnnotationValueStmt),
    CreateBranchStmt(CreateBranchStmt),
    CreateCastCommand(CreateCastCommand),
    CreateCastCommandsBlock(CreateCastCommandsBlock),
    CreateCastCommandsList(CreateCastCommandsList),
    CreateCastStmt(CreateCastStmt),
    CreateCommand(CreateCommand),
    CreateCommandsBlock(CreateCommandsBlock),
    CreateCommandsList(CreateCommandsList),
    CreateConcreteConstraintStmt(CreateConcreteConstraintStmt),
    CreateConcreteIndexSDLCommandFull(CreateConcreteIndexSDLCommandFull),
    CreateConcreteIndexSDLCommandShort(CreateConcreteIndexSDLCommandShort),
    CreateConcreteIndexSDLCommandsBlock(CreateConcreteIndexSDLCommandsBlock),
    CreateConcreteIndexSDLCommandsList(CreateConcreteIndexSDLCommandsList),
    CreateConcreteIndexStmt(CreateConcreteIndexStmt),
    CreateConcreteLinkCommand(CreateConcreteLinkCommand),
    CreateConcreteLinkCommandsBlock(CreateConcreteLinkCommandsBlock),
    CreateConcreteLinkCommandsList(CreateConcreteLinkCommandsList),
    CreateConcreteLinkSDLCommandBlock(CreateConcreteLinkSDLCommandBlock),
    CreateConcreteLinkSDLCommandFull(CreateConcreteLinkSDLCommandFull),
    CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort),
    CreateConcreteLinkSDLCommandsBlock(CreateConcreteLinkSDLCommandsBlock),
    CreateConcreteLinkSDLCommandsList(CreateConcreteLinkSDLCommandsList),
    CreateConcreteLinkStmt(CreateConcreteLinkStmt),
    CreateConcretePropertyCommand(CreateConcretePropertyCommand),
    CreateConcretePropertyCommandsBlock(CreateConcretePropertyCommandsBlock),
    CreateConcretePropertyCommandsList(CreateConcretePropertyCommandsList),
    CreateConcretePropertySDLCommandBlock(CreateConcretePropertySDLCommandBlock),
    CreateConcretePropertySDLCommandFull(CreateConcretePropertySDLCommandFull),
    CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort),
    CreateConcretePropertySDLCommandsBlock(CreateConcretePropertySDLCommandsBlock),
    CreateConcretePropertySDLCommandsList(CreateConcretePropertySDLCommandsList),
    CreateConcretePropertyStmt(CreateConcretePropertyStmt),
    CreateConstraintStmt(CreateConstraintStmt),
    CreateDatabaseCommand(CreateDatabaseCommand),
    CreateDatabaseCommandsBlock(CreateDatabaseCommandsBlock),
    CreateDatabaseCommandsList(CreateDatabaseCommandsList),
    CreateDatabaseStmt(CreateDatabaseStmt),
    CreateExtensionCommand(CreateExtensionCommand),
    CreateExtensionCommandsBlock(CreateExtensionCommandsBlock),
    CreateExtensionCommandsList(CreateExtensionCommandsList),
    CreateExtensionPackageCommand(CreateExtensionPackageCommand),
    CreateExtensionPackageCommandsBlock(CreateExtensionPackageCommandsBlock),
    CreateExtensionPackageCommandsList(CreateExtensionPackageCommandsList),
    CreateExtensionPackageStmt(CreateExtensionPackageStmt),
    CreateExtensionStmt(CreateExtensionStmt),
    CreateFunctionArgs(CreateFunctionArgs),
    CreateFunctionCommand(CreateFunctionCommand),
    CreateFunctionCommandsBlock(CreateFunctionCommandsBlock),
    CreateFunctionCommandsList(CreateFunctionCommandsList),
    CreateFunctionSDLCommandFull(CreateFunctionSDLCommandFull),
    CreateFunctionSDLCommandShort(CreateFunctionSDLCommandShort),
    CreateFunctionSDLCommandsBlock(CreateFunctionSDLCommandsBlock),
    CreateFunctionSDLCommandsList(CreateFunctionSDLCommandsList),
    CreateFunctionSingleSDLCommandBlock(CreateFunctionSingleSDLCommandBlock),
    CreateFunctionStmt(CreateFunctionStmt),
    CreateFutureStmt(CreateFutureStmt),
    CreateGlobalCommand(CreateGlobalCommand),
    CreateGlobalCommandsBlock(CreateGlobalCommandsBlock),
    CreateGlobalCommandsList(CreateGlobalCommandsList),
    CreateGlobalSDLCommandFull(CreateGlobalSDLCommandFull),
    CreateGlobalSDLCommandShort(CreateGlobalSDLCommandShort),
    CreateGlobalSDLCommandsBlock(CreateGlobalSDLCommandsBlock),
    CreateGlobalSDLCommandsList(CreateGlobalSDLCommandsList),
    CreateGlobalStmt(CreateGlobalStmt),
    CreateIndexCommand(CreateIndexCommand),
    CreateIndexCommandsBlock(CreateIndexCommandsBlock),
    CreateIndexCommandsList(CreateIndexCommandsList),
    CreateIndexMatchCommand(CreateIndexMatchCommand),
    CreateIndexMatchCommandsBlock(CreateIndexMatchCommandsBlock),
    CreateIndexMatchCommandsList(CreateIndexMatchCommandsList),
    CreateIndexMatchStmt(CreateIndexMatchStmt),
    CreateIndexSDLCommandFull(CreateIndexSDLCommandFull),
    CreateIndexSDLCommandShort(CreateIndexSDLCommandShort),
    CreateIndexSDLCommandsBlock(CreateIndexSDLCommandsBlock),
    CreateIndexSDLCommandsList(CreateIndexSDLCommandsList),
    CreateIndexStmt(CreateIndexStmt),
    CreateLinkCommand(CreateLinkCommand),
    CreateLinkCommandsBlock(CreateLinkCommandsBlock),
    CreateLinkCommandsList(CreateLinkCommandsList),
    CreateLinkSDLCommandBlock(CreateLinkSDLCommandBlock),
    CreateLinkSDLCommandFull(CreateLinkSDLCommandFull),
    CreateLinkSDLCommandShort(CreateLinkSDLCommandShort),
    CreateLinkSDLCommandsBlock(CreateLinkSDLCommandsBlock),
    CreateLinkSDLCommandsList(CreateLinkSDLCommandsList),
    CreateLinkStmt(CreateLinkStmt),
    CreateMigrationCommand(CreateMigrationCommand),
    CreateMigrationCommandsBlock(CreateMigrationCommandsBlock),
    CreateMigrationCommandsList(CreateMigrationCommandsList),
    CreateMigrationStmt(CreateMigrationStmt),
    CreateModuleStmt(CreateModuleStmt),
    CreateObjectTypeCommand(CreateObjectTypeCommand),
    CreateObjectTypeCommandsBlock(CreateObjectTypeCommandsBlock),
    CreateObjectTypeCommandsList(CreateObjectTypeCommandsList),
    CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock),
    CreateObjectTypeSDLCommandFull(CreateObjectTypeSDLCommandFull),
    CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort),
    CreateObjectTypeSDLCommandsBlock(CreateObjectTypeSDLCommandsBlock),
    CreateObjectTypeSDLCommandsList(CreateObjectTypeSDLCommandsList),
    CreateObjectTypeStmt(CreateObjectTypeStmt),
    CreateOperatorCommand(CreateOperatorCommand),
    CreateOperatorCommandsBlock(CreateOperatorCommandsBlock),
    CreateOperatorCommandsList(CreateOperatorCommandsList),
    CreateOperatorStmt(CreateOperatorStmt),
    CreatePropertyCommand(CreatePropertyCommand),
    CreatePropertyCommandsBlock(CreatePropertyCommandsBlock),
    CreatePropertyCommandsList(CreatePropertyCommandsList),
    CreatePropertySDLCommandFull(CreatePropertySDLCommandFull),
    CreatePropertySDLCommandShort(CreatePropertySDLCommandShort),
    CreatePropertySDLCommandsBlock(CreatePropertySDLCommandsBlock),
    CreatePropertySDLCommandsList(CreatePropertySDLCommandsList),
    CreatePropertyStmt(CreatePropertyStmt),
    CreatePseudoTypeCommand(CreatePseudoTypeCommand),
    CreatePseudoTypeCommandsBlock(CreatePseudoTypeCommandsBlock),
    CreatePseudoTypeCommandsList(CreatePseudoTypeCommandsList),
    CreatePseudoTypeStmt(CreatePseudoTypeStmt),
    CreateRewriteCommand(CreateRewriteCommand),
    CreateRewriteCommandsBlock(CreateRewriteCommandsBlock),
    CreateRewriteCommandsList(CreateRewriteCommandsList),
    CreateRewriteSDLCommandFull(CreateRewriteSDLCommandFull),
    CreateRewriteSDLCommandShort(CreateRewriteSDLCommandShort),
    CreateRewriteSDLCommandsBlock(CreateRewriteSDLCommandsBlock),
    CreateRewriteSDLCommandsList(CreateRewriteSDLCommandsList),
    CreateRewriteStmt(CreateRewriteStmt),
    CreateRoleCommand(CreateRoleCommand),
    CreateRoleCommandsBlock(CreateRoleCommandsBlock),
    CreateRoleCommandsList(CreateRoleCommandsList),
    CreateRoleStmt(CreateRoleStmt),
    CreateSDLCommandFull(CreateSDLCommandFull),
    CreateSDLCommandShort(CreateSDLCommandShort),
    CreateSDLCommandsBlock(CreateSDLCommandsBlock),
    CreateSDLCommandsList(CreateSDLCommandsList),
    CreateScalarTypeCommand(CreateScalarTypeCommand),
    CreateScalarTypeCommandsBlock(CreateScalarTypeCommandsBlock),
    CreateScalarTypeCommandsList(CreateScalarTypeCommandsList),
    CreateScalarTypeSDLCommandBlock(CreateScalarTypeSDLCommandBlock),
    CreateScalarTypeSDLCommandFull(CreateScalarTypeSDLCommandFull),
    CreateScalarTypeSDLCommandShort(CreateScalarTypeSDLCommandShort),
    CreateScalarTypeSDLCommandsBlock(CreateScalarTypeSDLCommandsBlock),
    CreateScalarTypeSDLCommandsList(CreateScalarTypeSDLCommandsList),
    CreateScalarTypeStmt(CreateScalarTypeStmt),
    CreateSimpleExtending(CreateSimpleExtending),
    CreateTriggerCommand(CreateTriggerCommand),
    CreateTriggerCommandsBlock(CreateTriggerCommandsBlock),
    CreateTriggerCommandsList(CreateTriggerCommandsList),
    CreateTriggerSDLCommandFull(CreateTriggerSDLCommandFull),
    CreateTriggerSDLCommandShort(CreateTriggerSDLCommandShort),
    CreateTriggerSDLCommandsBlock(CreateTriggerSDLCommandsBlock),
    CreateTriggerSDLCommandsList(CreateTriggerSDLCommandsList),
    CreateTriggerStmt(CreateTriggerStmt),
    DDLStmt(DDLStmt),
    DDLWithBlock(DDLWithBlock),
    DatabaseName(DatabaseName),
    DatabaseStmt(DatabaseStmt),
    DescribeFormat(DescribeFormat),
    DescribeStmt(DescribeStmt),
    DotName(DotName),
    DottedIdents(DottedIdents),
    DropAccessPolicyStmt(DropAccessPolicyStmt),
    DropAliasStmt(DropAliasStmt),
    DropAnnotationStmt(DropAnnotationStmt),
    DropAnnotationValueStmt(DropAnnotationValueStmt),
    DropBranchStmt(DropBranchStmt),
    DropCastStmt(DropCastStmt),
    DropConcreteConstraintStmt(DropConcreteConstraintStmt),
    DropConcreteIndexCommand(DropConcreteIndexCommand),
    DropConcreteIndexCommandsBlock(DropConcreteIndexCommandsBlock),
    DropConcreteIndexCommandsList(DropConcreteIndexCommandsList),
    DropConcreteIndexStmt(DropConcreteIndexStmt),
    DropConcreteLinkCommand(DropConcreteLinkCommand),
    DropConcreteLinkCommandsBlock(DropConcreteLinkCommandsBlock),
    DropConcreteLinkCommandsList(DropConcreteLinkCommandsList),
    DropConcreteLinkStmt(DropConcreteLinkStmt),
    DropConcretePropertyStmt(DropConcretePropertyStmt),
    DropConstraintStmt(DropConstraintStmt),
    DropDatabaseStmt(DropDatabaseStmt),
    DropExtensionPackageStmt(DropExtensionPackageStmt),
    DropExtensionStmt(DropExtensionStmt),
    DropFunctionStmt(DropFunctionStmt),
    DropFutureStmt(DropFutureStmt),
    DropGlobalStmt(DropGlobalStmt),
    DropIndexMatchStmt(DropIndexMatchStmt),
    DropIndexStmt(DropIndexStmt),
    DropLinkCommand(DropLinkCommand),
    DropLinkCommandsBlock(DropLinkCommandsBlock),
    DropLinkCommandsList(DropLinkCommandsList),
    DropLinkStmt(DropLinkStmt),
    DropMigrationStmt(DropMigrationStmt),
    DropModuleStmt(DropModuleStmt),
    DropObjectTypeCommand(DropObjectTypeCommand),
    DropObjectTypeCommandsBlock(DropObjectTypeCommandsBlock),
    DropObjectTypeCommandsList(DropObjectTypeCommandsList),
    DropObjectTypeStmt(DropObjectTypeStmt),
    DropOperatorStmt(DropOperatorStmt),
    DropPropertyStmt(DropPropertyStmt),
    DropRewriteStmt(DropRewriteStmt),
    DropRoleStmt(DropRoleStmt),
    DropScalarTypeStmt(DropScalarTypeStmt),
    DropTriggerStmt(DropTriggerStmt),
    EdgeQLBlock(EdgeQLBlock),
    EdgeQLGrammar(EdgeQLGrammar),
    Expr(Expr),
    ExprList(ExprList),
    ExprListInner(ExprListInner),
    ExprStmt(ExprStmt),
    ExprStmtCore(ExprStmtCore),
    Extending(Extending),
    ExtendingSimple(ExtendingSimple),
    ExtensionPackageStmt(ExtensionPackageStmt),
    ExtensionRequirementDeclaration(ExtensionRequirementDeclaration),
    ExtensionStmt(ExtensionStmt),
    ExtensionVersion(ExtensionVersion),
    FilterClause(FilterClause),
    FreeComputableShapePointer(FreeComputableShapePointer),
    FreeComputableShapePointerList(FreeComputableShapePointerList),
    FreeComputableShapePointerListInner(FreeComputableShapePointerListInner),
    FreeShape(FreeShape),
    FreeSimpleShapePointer(FreeSimpleShapePointer),
    FreeStepName(FreeStepName),
    FromFunction(FromFunction),
    FullTypeExpr(FullTypeExpr),
    FuncApplication(FuncApplication),
    FuncArgList(FuncArgList),
    FuncArgListInner(FuncArgListInner),
    FuncCallArg(FuncCallArg),
    FuncCallArgExpr(FuncCallArgExpr),
    FuncDeclArg(FuncDeclArg),
    FuncDeclArgList(FuncDeclArgList),
    FuncDeclArgListInner(FuncDeclArgListInner),
    FuncDeclArgName(FuncDeclArgName),
    FuncDeclArgs(FuncDeclArgs),
    FuncExpr(FuncExpr),
    FunctionDeclaration(FunctionDeclaration),
    FunctionDeclarationShort(FunctionDeclarationShort),
    FunctionType(FunctionType),
    FutureRequirementDeclaration(FutureRequirementDeclaration),
    FutureStmt(FutureStmt),
    GlobalDeclaration(GlobalDeclaration),
    GlobalDeclarationShort(GlobalDeclarationShort),
    GroupingAtom(GroupingAtom),
    GroupingAtomList(GroupingAtomList),
    GroupingAtomListInner(GroupingAtomListInner),
    GroupingElement(GroupingElement),
    GroupingElementList(GroupingElementList),
    GroupingElementListInner(GroupingElementListInner),
    GroupingIdent(GroupingIdent),
    GroupingIdentList(GroupingIdentList),
    Identifier(Identifier),
    IfThenElseExpr(IfThenElseExpr),
    IndexArg(IndexArg),
    IndexArgList(IndexArgList),
    IndexArgListInner(IndexArgListInner),
    IndexDeclaration(IndexDeclaration),
    IndexDeclarationShort(IndexDeclarationShort),
    IndexExtArgList(IndexExtArgList),
    IndirectionEl(IndirectionEl),
    InnerDDLStmt(InnerDDLStmt),
    InternalGroup(InternalGroup),
    LimitClause(LimitClause),
    LinkDeclaration(LinkDeclaration),
    LinkDeclarationShort(LinkDeclarationShort),
    MigrationStmt(MigrationStmt),
    ModuleDeclaration(ModuleDeclaration),
    ModuleName(ModuleName),
    NamedTuple(NamedTuple),
    NamedTupleElement(NamedTupleElement),
    NamedTupleElementList(NamedTupleElementList),
    NamedTupleElementListInner(NamedTupleElementListInner),
    NestedQLBlockStmt(NestedQLBlockStmt),
    NodeName(NodeName),
    NontrivialTypeExpr(NontrivialTypeExpr),
    ObjectTypeDeclaration(ObjectTypeDeclaration),
    ObjectTypeDeclarationShort(ObjectTypeDeclarationShort),
    OffsetClause(OffsetClause),
    OnExpr(OnExpr),
    OnSourceDeleteResetStmt(OnSourceDeleteResetStmt),
    OnSourceDeleteStmt(OnSourceDeleteStmt),
    OnTargetDeleteResetStmt(OnTargetDeleteResetStmt),
    OnTargetDeleteStmt(OnTargetDeleteStmt),
    OperatorCode(OperatorCode),
    OperatorKind(OperatorKind),
    OptAlterUsingClause(OptAlterUsingClause),
    OptAnySubShape(OptAnySubShape),
    OptConcreteConstraintArgList(OptConcreteConstraintArgList),
    OptCreateAccessPolicyCommandsBlock(OptCreateAccessPolicyCommandsBlock),
    OptCreateAnnotationCommandsBlock(OptCreateAnnotationCommandsBlock),
    OptCreateCommandsBlock(OptCreateCommandsBlock),
    OptCreateConcreteLinkCommandsBlock(OptCreateConcreteLinkCommandsBlock),
    OptCreateConcretePropertyCommandsBlock(OptCreateConcretePropertyCommandsBlock),
    OptCreateDatabaseCommandsBlock(OptCreateDatabaseCommandsBlock),
    OptCreateExtensionCommandsBlock(OptCreateExtensionCommandsBlock),
    OptCreateExtensionPackageCommandsBlock(OptCreateExtensionPackageCommandsBlock),
    OptCreateGlobalCommandsBlock(OptCreateGlobalCommandsBlock),
    OptCreateIndexCommandsBlock(OptCreateIndexCommandsBlock),
    OptCreateIndexMatchCommandsBlock(OptCreateIndexMatchCommandsBlock),
    OptCreateLinkCommandsBlock(OptCreateLinkCommandsBlock),
    OptCreateMigrationCommandsBlock(OptCreateMigrationCommandsBlock),
    OptCreateObjectTypeCommandsBlock(OptCreateObjectTypeCommandsBlock),
    OptCreateOperatorCommandsBlock(OptCreateOperatorCommandsBlock),
    OptCreatePropertyCommandsBlock(OptCreatePropertyCommandsBlock),
    OptCreatePseudoTypeCommandsBlock(OptCreatePseudoTypeCommandsBlock),
    OptCreateRewriteCommandsBlock(OptCreateRewriteCommandsBlock),
    OptCreateRoleCommandsBlock(OptCreateRoleCommandsBlock),
    OptCreateScalarTypeCommandsBlock(OptCreateScalarTypeCommandsBlock),
    OptCreateTriggerCommandsBlock(OptCreateTriggerCommandsBlock),
    OptDefault(OptDefault),
    OptDeferred(OptDeferred),
    OptDelegated(OptDelegated),
    OptDirection(OptDirection),
    OptDropConcreteIndexCommandsBlock(OptDropConcreteIndexCommandsBlock),
    OptDropConcreteLinkCommandsBlock(OptDropConcreteLinkCommandsBlock),
    OptDropLinkCommandsBlock(OptDropLinkCommandsBlock),
    OptDropObjectTypeCommandsBlock(OptDropObjectTypeCommandsBlock),
    OptExceptExpr(OptExceptExpr),
    OptExprList(OptExprList),
    OptExtending(OptExtending),
    OptExtendingSimple(OptExtendingSimple),
    OptExtensionVersion(OptExtensionVersion),
    OptFilterClause(OptFilterClause),
    OptFuncArgList(OptFuncArgList),
    OptGroupingAlias(OptGroupingAlias),
    OptIfNotExists(OptIfNotExists),
    OptIndexArgList(OptIndexArgList),
    OptIndexExtArgList(OptIndexExtArgList),
    OptMigrationNameParentName(OptMigrationNameParentName),
    OptNonesOrder(OptNonesOrder),
    OptOnExpr(OptOnExpr),
    OptParameterKind(OptParameterKind),
    OptPosCallArgList(OptPosCallArgList),
    OptPosition(OptPosition),
    OptPtrQuals(OptPtrQuals),
    OptPtrTarget(OptPtrTarget),
    OptSelectLimit(OptSelectLimit),
    OptSemicolons(OptSemicolons),
    OptShortExtending(OptShortExtending),
    OptSortClause(OptSortClause),
    OptSuperuser(OptSuperuser),
    OptTransactionModeList(OptTransactionModeList),
    OptTypeIntersection(OptTypeIntersection),
    OptTypeQualifier(OptTypeQualifier),
    OptUnlessConflictClause(OptUnlessConflictClause),
    OptUsingBlock(OptUsingBlock),
    OptUsingClause(OptUsingClause),
    OptWhenBlock(OptWhenBlock),
    OptWithDDLStmt(OptWithDDLStmt),
    OptionalOptional(OptionalOptional),
    OptionallyAliasedExpr(OptionallyAliasedExpr),
    OrderbyExpr(OrderbyExpr),
    OrderbyList(OrderbyList),
    ParameterKind(ParameterKind),
    ParenExpr(ParenExpr),
    ParenTypeExpr(ParenTypeExpr),
    PartialReservedKeyword(PartialReservedKeyword),
    Path(Path),
    PathNodeName(PathNodeName),
    PathStep(PathStep),
    PathStepName(PathStepName),
    PointerName(PointerName),
    PopulateMigrationStmt(PopulateMigrationStmt),
    PosCallArg(PosCallArg),
    PosCallArgList(PosCallArgList),
    PropertyDeclaration(PropertyDeclaration),
    PropertyDeclarationShort(PropertyDeclarationShort),
    PtrIdentifier(PtrIdentifier),
    PtrName(PtrName),
    PtrNodeName(PtrNodeName),
    PtrQualifiedNodeName(PtrQualifiedNodeName),
    PtrQuals(PtrQuals),
    PtrTarget(PtrTarget),
    QualifiedName(QualifiedName),
    RenameStmt(RenameStmt),
    ReservedKeyword(ReservedKeyword),
    ResetFieldStmt(ResetFieldStmt),
    ResetSchemaStmt(ResetSchemaStmt),
    ResetStmt(ResetStmt),
    RewriteDeclarationBlock(RewriteDeclarationBlock),
    RewriteDeclarationShort(RewriteDeclarationShort),
    RewriteKind(RewriteKind),
    RewriteKindList(RewriteKindList),
    RoleStmt(RoleStmt),
    SDLBlockStatement(SDLBlockStatement),
    SDLCommandBlock(SDLCommandBlock),
    SDLDocument(SDLDocument),
    SDLShortStatement(SDLShortStatement),
    SDLStatement(SDLStatement),
    SDLStatements(SDLStatements),
    ScalarTypeDeclaration(ScalarTypeDeclaration),
    ScalarTypeDeclarationShort(ScalarTypeDeclarationShort),
    SchemaItem(SchemaItem),
    SchemaObjectClass(SchemaObjectClass),
    SelectLimit(SelectLimit),
    Semicolons(Semicolons),
    SessionStmt(SessionStmt),
    Set(Set),
    SetAnnotation(SetAnnotation),
    SetCardinalityStmt(SetCardinalityStmt),
    SetDelegatedStmt(SetDelegatedStmt),
    SetField(SetField),
    SetFieldStmt(SetFieldStmt),
    SetGlobalTypeStmt(SetGlobalTypeStmt),
    SetPointerTypeStmt(SetPointerTypeStmt),
    SetRequiredInCreateStmt(SetRequiredInCreateStmt),
    SetRequiredStmt(SetRequiredStmt),
    SetStmt(SetStmt),
    Shape(Shape),
    ShapeElement(ShapeElement),
    ShapeElementList(ShapeElementList),
    ShapeElementListInner(ShapeElementListInner),
    ShapePath(ShapePath),
    ShapePointer(ShapePointer),
    ShortExtending(ShortExtending),
    ShortNodeName(ShortNodeName),
    ShortNodeNameList(ShortNodeNameList),
    SimpleDelete(SimpleDelete),
    SimpleFor(SimpleFor),
    SimpleGroup(SimpleGroup),
    SimpleInsert(SimpleInsert),
    SimpleSelect(SimpleSelect),
    SimpleShapePath(SimpleShapePath),
    SimpleShapePointer(SimpleShapePointer),
    SimpleTypeName(SimpleTypeName),
    SimpleTypeNameList(SimpleTypeNameList),
    SimpleUpdate(SimpleUpdate),
    SingleStatement(SingleStatement),
    SortClause(SortClause),
    Splat(Splat),
    StartMigrationStmt(StartMigrationStmt),
    StatementBlock(StatementBlock),
    Stmt(Stmt),
    Subtype(Subtype),
    SubtypeList(SubtypeList),
    SubtypeListInner(SubtypeListInner),
    TransactionMode(TransactionMode),
    TransactionModeList(TransactionModeList),
    TransactionStmt(TransactionStmt),
    TriggerDeclarationBlock(TriggerDeclarationBlock),
    TriggerDeclarationShort(TriggerDeclarationShort),
    TriggerKind(TriggerKind),
    TriggerKindList(TriggerKindList),
    TriggerScope(TriggerScope),
    TriggerTiming(TriggerTiming),
    Tuple(Tuple),
    TypeExpr(TypeExpr),
    TypeIntersection(TypeIntersection),
    TypeName(TypeName),
    TypeNameList(TypeNameList),
    UnlessConflictCause(UnlessConflictCause),
    UnlessConflictSpecifier(UnlessConflictSpecifier),
    UnqualifiedPointerName(UnqualifiedPointerName),
    UnreservedKeyword(UnreservedKeyword),
    Using(Using),
    UsingClause(UsingClause),
    UsingStmt(UsingStmt),
    WithBlock(WithBlock),
    WithDDLStmt(WithDDLStmt),
    WithDecl(WithDecl),
    WithDeclList(WithDeclList),
    WithDeclListInner(WithDeclListInner),
}

#[derive(Debug, Clone)]
pub enum AbortMigrationStmt {
    ABORT_MIGRATION,
    ABORT_MIGRATION_REWRITE,
}

#[derive(Debug, Clone)]
pub enum AccessKind {
    ALL,
    DELETE,
    INSERT,
    SELECT,
    UPDATE,
    UPDATE_READ,
    UPDATE_WRITE,
}

#[derive(Debug, Clone)]
pub enum AccessKindList {
    AccessKind,
    AccessKindList_COMMA_AccessKind,
}

#[derive(Debug, Clone)]
pub enum AccessPermStmt {
    AccessPolicyAction_AccessKindList,
}

#[derive(Debug, Clone)]
pub enum AccessPolicyAction {
    ALLOW,
    DENY,
}

#[derive(Debug, Clone)]
pub enum AccessPolicyDeclarationBlock {
    ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_CreateAccessPolicySDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AccessPolicyDeclarationShort {
    ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock,
}

#[derive(Debug, Clone)]
pub enum AccessUsingStmt {
    RESET_EXPRESSION,
    USING_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum AccessWhenStmt {
    RESET_WHEN,
    WHEN_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum AdministerStmt {
    ADMINISTER_FuncExpr,
}

#[derive(Debug, Clone)]
pub enum AliasDecl {
    AliasedExpr,
    Identifier_AS_MODULE_ModuleName,
    MODULE_ModuleName,
}

#[derive(Debug, Clone)]
pub enum AliasDeclaration {
    ALIAS_NodeName_CreateAliasSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AliasDeclarationShort {
    ALIAS_NodeName_CreateAliasSingleSDLCommandBlock,
    ALIAS_NodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum AliasedExpr {
    Identifier_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum AliasedExprList {
    AliasedExprListInner,
    AliasedExprListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum AliasedExprListInner {
    AliasedExpr,
    AliasedExprListInner_COMMA_AliasedExpr,
}

#[derive(Debug, Clone)]
pub enum AlterAbstract {
    DROP_ABSTRACT,
    RESET_ABSTRACT,
    SET_ABSTRACT,
    SET_NOT_ABSTRACT,
}

#[derive(Debug, Clone)]
pub enum AlterAccessPolicyCommand {
    AccessPermStmt,
    AccessUsingStmt,
    AccessWhenStmt,
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterAccessPolicyCommandsBlock {
    AlterAccessPolicyCommand,
    LBRACE_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterAccessPolicyCommandsList {
    AlterAccessPolicyCommand,
    AlterAccessPolicyCommandsList_Semicolons_AlterAccessPolicyCommand,
}

#[derive(Debug, Clone)]
pub enum AlterAccessPolicyStmt {
    ALTER_ACCESS_POLICY_UnqualifiedPointerName_AlterAccessPolicyCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterAliasCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterAliasCommandsBlock {
    AlterAliasCommand,
    LBRACE_AlterAliasCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterAliasCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterAliasCommandsList {
    AlterAliasCommand,
    AlterAliasCommandsList_Semicolons_AlterAliasCommand,
}

#[derive(Debug, Clone)]
pub enum AlterAliasStmt {
    ALTER_ALIAS_NodeName_AlterAliasCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterAnnotationCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
}

#[derive(Debug, Clone)]
pub enum AlterAnnotationCommandsBlock {
    AlterAnnotationCommand,
    LBRACE_AlterAnnotationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterAnnotationCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterAnnotationCommandsList {
    AlterAnnotationCommand,
    AlterAnnotationCommandsList_Semicolons_AlterAnnotationCommand,
}

#[derive(Debug, Clone)]
pub enum AlterAnnotationStmt {
    ALTER_ABSTRACT_ANNOTATION_NodeName_AlterAnnotationCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterAnnotationValueStmt {
    ALTER_ANNOTATION_NodeName_ASSIGN_Expr,
    ALTER_ANNOTATION_NodeName_DROP_OWNED,
}

#[derive(Debug, Clone)]
pub enum AlterBranchCommand {
    RenameStmt,
}

#[derive(Debug, Clone)]
pub enum AlterBranchCommandsBlock {
    AlterBranchCommand,
    LBRACE_AlterBranchCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterBranchCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterBranchCommandsList {
    AlterBranchCommand,
    AlterBranchCommandsList_Semicolons_AlterBranchCommand,
}

#[derive(Debug, Clone)]
pub enum AlterBranchStmt {
    ALTER_BRANCH_DatabaseName_BranchOptions_AlterBranchCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterCastCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterCastCommandsBlock {
    AlterCastCommand,
    LBRACE_AlterCastCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterCastCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterCastCommandsList {
    AlterCastCommand,
    AlterCastCommandsList_Semicolons_AlterCastCommand,
}

#[derive(Debug, Clone)]
pub enum AlterCastStmt {
    ALTER_CAST_FROM_TypeName_TO_TypeName_AlterCastCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterCommandsBlock {
    AlterCommand,
    LBRACE_AlterCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterCommandsList {
    AlterCommand,
    AlterCommandsList_Semicolons_AlterCommand,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteConstraintCommand {
    AlterAbstract,
    AlterAnnotationValueStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetDelegatedStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteConstraintCommandsBlock {
    AlterConcreteConstraintCommand,
    LBRACE_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteConstraintCommandsList {
    AlterConcreteConstraintCommand,
    AlterConcreteConstraintCommandsList_Semicolons_AlterConcreteConstraintCommand,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteConstraintStmt {
    ALTER_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_AlterConcreteConstraintCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteIndexCommand {
    AlterAnnotationValueStmt,
    AlterDeferredStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteIndexCommandsBlock {
    AlterConcreteIndexCommand,
    LBRACE_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteIndexCommandsList {
    AlterConcreteIndexCommand,
    AlterConcreteIndexCommandsList_Semicolons_AlterConcreteIndexCommand,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteIndexStmt {
    ALTER_INDEX_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock,
    ALTER_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteLinkCommand {
    AlterAnnotationValueStmt,
    AlterConcreteConstraintStmt,
    AlterConcreteIndexStmt,
    AlterConcretePropertyStmt,
    AlterOwnedStmt,
    AlterRewriteStmt,
    AlterSimpleExtending,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    CreateConcretePropertyStmt,
    CreateRewriteStmt,
    DropAnnotationValueStmt,
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcretePropertyStmt,
    DropRewriteStmt,
    OnSourceDeleteResetStmt,
    OnSourceDeleteStmt,
    OnTargetDeleteResetStmt,
    OnTargetDeleteStmt,
    RenameStmt,
    ResetFieldStmt,
    SetCardinalityStmt,
    SetFieldStmt,
    SetPointerTypeStmt,
    SetRequiredStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteLinkCommandsBlock {
    AlterConcreteLinkCommand,
    LBRACE_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteLinkCommandsList {
    AlterConcreteLinkCommand,
    AlterConcreteLinkCommandsList_Semicolons_AlterConcreteLinkCommand,
}

#[derive(Debug, Clone)]
pub enum AlterConcreteLinkStmt {
    ALTER_LINK_UnqualifiedPointerName_AlterConcreteLinkCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterConcretePropertyCommand {
    AlterAnnotationValueStmt,
    AlterConcreteConstraintStmt,
    AlterOwnedStmt,
    AlterRewriteStmt,
    AlterSimpleExtending,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateRewriteStmt,
    DropAnnotationValueStmt,
    DropConcreteConstraintStmt,
    DropRewriteStmt,
    RenameStmt,
    ResetFieldStmt,
    SetCardinalityStmt,
    SetFieldStmt,
    SetPointerTypeStmt,
    SetRequiredStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterConcretePropertyCommandsBlock {
    AlterConcretePropertyCommand,
    LBRACE_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterConcretePropertyCommandsList {
    AlterConcretePropertyCommand,
    AlterConcretePropertyCommandsList_Semicolons_AlterConcretePropertyCommand,
}

#[derive(Debug, Clone)]
pub enum AlterConcretePropertyStmt {
    ALTER_PROPERTY_UnqualifiedPointerName_AlterConcretePropertyCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterConstraintStmt {
    ALTER_ABSTRACT_CONSTRAINT_NodeName_AlterCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterCurrentMigrationStmt {
    ALTER_CURRENT_MIGRATION_REJECT_PROPOSED,
}

#[derive(Debug, Clone)]
pub enum AlterDatabaseCommand {
    RenameStmt,
}

#[derive(Debug, Clone)]
pub enum AlterDatabaseCommandsBlock {
    AlterDatabaseCommand,
    LBRACE_AlterDatabaseCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterDatabaseCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterDatabaseCommandsList {
    AlterDatabaseCommand,
    AlterDatabaseCommandsList_Semicolons_AlterDatabaseCommand,
}

#[derive(Debug, Clone)]
pub enum AlterDatabaseStmt {
    ALTER_DATABASE_DatabaseName_AlterDatabaseCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterDeferredStmt {
    DROP_DEFERRED,
    SET_DEFERRED,
}

#[derive(Debug, Clone)]
pub enum AlterExtending {
    AlterAbstract,
    DROP_EXTENDING_TypeNameList,
    EXTENDING_TypeNameList_OptPosition,
}

#[derive(Debug, Clone)]
pub enum AlterFunctionCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    FromFunction,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterFunctionCommandsBlock {
    AlterFunctionCommand,
    LBRACE_AlterFunctionCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterFunctionCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterFunctionCommandsList {
    AlterFunctionCommand,
    AlterFunctionCommandsList_Semicolons_AlterFunctionCommand,
}

#[derive(Debug, Clone)]
pub enum AlterFunctionStmt {
    ALTER_FUNCTION_NodeName_CreateFunctionArgs_AlterFunctionCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterGlobalCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetCardinalityStmt,
    SetFieldStmt,
    SetGlobalTypeStmt,
    SetRequiredStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterGlobalCommandsBlock {
    AlterGlobalCommand,
    LBRACE_AlterGlobalCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterGlobalCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterGlobalCommandsList {
    AlterGlobalCommand,
    AlterGlobalCommandsList_Semicolons_AlterGlobalCommand,
}

#[derive(Debug, Clone)]
pub enum AlterGlobalStmt {
    ALTER_GLOBAL_NodeName_AlterGlobalCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterIndexCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterIndexCommandsBlock {
    AlterIndexCommand,
    LBRACE_AlterIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterIndexCommandsList {
    AlterIndexCommand,
    AlterIndexCommandsList_Semicolons_AlterIndexCommand,
}

#[derive(Debug, Clone)]
pub enum AlterIndexStmt {
    ALTER_ABSTRACT_INDEX_NodeName_AlterIndexCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterLinkCommand {
    AlterAnnotationValueStmt,
    AlterConcreteConstraintStmt,
    AlterConcreteIndexStmt,
    AlterConcretePropertyStmt,
    AlterRewriteStmt,
    AlterSimpleExtending,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    CreateConcretePropertyStmt,
    CreateRewriteStmt,
    DropAnnotationValueStmt,
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcretePropertyStmt,
    DropRewriteStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterLinkCommandsBlock {
    AlterLinkCommand,
    LBRACE_AlterLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterLinkCommandsList {
    AlterLinkCommand,
    AlterLinkCommandsList_Semicolons_AlterLinkCommand,
}

#[derive(Debug, Clone)]
pub enum AlterLinkStmt {
    ALTER_ABSTRACT_LINK_PtrNodeName_AlterLinkCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterMigrationCommand {
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterMigrationCommandsBlock {
    AlterMigrationCommand,
    LBRACE_AlterMigrationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterMigrationCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterMigrationCommandsList {
    AlterMigrationCommand,
    AlterMigrationCommandsList_Semicolons_AlterMigrationCommand,
}

#[derive(Debug, Clone)]
pub enum AlterMigrationStmt {
    ALTER_MIGRATION_NodeName_AlterMigrationCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterModuleStmt {
    ALTER_MODULE_ModuleName_AlterCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterObjectTypeCommand {
    AlterAccessPolicyStmt,
    AlterAnnotationValueStmt,
    AlterConcreteConstraintStmt,
    AlterConcreteIndexStmt,
    AlterConcreteLinkStmt,
    AlterConcretePropertyStmt,
    AlterSimpleExtending,
    AlterTriggerStmt,
    CreateAccessPolicyStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    CreateConcreteLinkStmt,
    CreateConcretePropertyStmt,
    CreateTriggerStmt,
    DropAccessPolicyStmt,
    DropAnnotationValueStmt,
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcreteLinkStmt,
    DropConcretePropertyStmt,
    DropTriggerStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterObjectTypeCommandsBlock {
    AlterObjectTypeCommand,
    LBRACE_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterObjectTypeCommandsList {
    AlterObjectTypeCommand,
    AlterObjectTypeCommandsList_Semicolons_AlterObjectTypeCommand,
}

#[derive(Debug, Clone)]
pub enum AlterObjectTypeStmt {
    ALTER_TYPE_NodeName_AlterObjectTypeCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterOperatorCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterOperatorCommandsBlock {
    AlterOperatorCommand,
    LBRACE_AlterOperatorCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterOperatorCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterOperatorCommandsList {
    AlterOperatorCommand,
    AlterOperatorCommandsList_Semicolons_AlterOperatorCommand,
}

#[derive(Debug, Clone)]
pub enum AlterOperatorStmt {
    ALTER_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_AlterOperatorCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterOwnedStmt {
    DROP_OWNED,
    SET_OWNED,
}

#[derive(Debug, Clone)]
pub enum AlterPropertyCommand {
    AlterAnnotationValueStmt,
    AlterRewriteStmt,
    CreateAnnotationValueStmt,
    CreateRewriteStmt,
    DropAnnotationValueStmt,
    DropRewriteStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterPropertyCommandsBlock {
    AlterPropertyCommand,
    LBRACE_AlterPropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterPropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterPropertyCommandsList {
    AlterPropertyCommand,
    AlterPropertyCommandsList_Semicolons_AlterPropertyCommand,
}

#[derive(Debug, Clone)]
pub enum AlterPropertyStmt {
    ALTER_ABSTRACT_PROPERTY_PtrNodeName_AlterPropertyCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterRewriteCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterRewriteCommandsBlock {
    AlterRewriteCommand,
    LBRACE_AlterRewriteCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterRewriteCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterRewriteCommandsList {
    AlterRewriteCommand,
    AlterRewriteCommandsList_Semicolons_AlterRewriteCommand,
}

#[derive(Debug, Clone)]
pub enum AlterRewriteStmt {
    ALTER_REWRITE_RewriteKindList_AlterRewriteCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterRoleCommand {
    AlterRoleExtending,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterRoleCommandsBlock {
    AlterRoleCommand,
    LBRACE_AlterRoleCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterRoleCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterRoleCommandsList {
    AlterRoleCommand,
    AlterRoleCommandsList_Semicolons_AlterRoleCommand,
}

#[derive(Debug, Clone)]
pub enum AlterRoleExtending {
    DROP_EXTENDING_ShortNodeNameList,
    EXTENDING_ShortNodeNameList_OptPosition,
}

#[derive(Debug, Clone)]
pub enum AlterRoleStmt {
    ALTER_ROLE_ShortNodeName_AlterRoleCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterScalarTypeCommand {
    AlterAnnotationValueStmt,
    AlterConcreteConstraintStmt,
    AlterExtending,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    DropAnnotationValueStmt,
    DropConcreteConstraintStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum AlterScalarTypeCommandsBlock {
    AlterScalarTypeCommand,
    LBRACE_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterScalarTypeCommandsList {
    AlterScalarTypeCommand,
    AlterScalarTypeCommandsList_Semicolons_AlterScalarTypeCommand,
}

#[derive(Debug, Clone)]
pub enum AlterScalarTypeStmt {
    ALTER_SCALAR_TYPE_NodeName_AlterScalarTypeCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AlterSimpleExtending {
    AlterAbstract,
    DROP_EXTENDING_SimpleTypeNameList,
    EXTENDING_SimpleTypeNameList_OptPosition,
}

#[derive(Debug, Clone)]
pub enum AlterTriggerCommand {
    AccessWhenStmt,
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum AlterTriggerCommandsBlock {
    AlterTriggerCommand,
    LBRACE_AlterTriggerCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterTriggerCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum AlterTriggerCommandsList {
    AlterTriggerCommand,
    AlterTriggerCommandsList_Semicolons_AlterTriggerCommand,
}

#[derive(Debug, Clone)]
pub enum AlterTriggerStmt {
    ALTER_TRIGGER_UnqualifiedPointerName_AlterTriggerCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AnalyzeStmt {
    ANALYZE_ExprStmt,
    ANALYZE_NamedTuple_ExprStmt,
}

#[derive(Debug, Clone)]
pub enum AnnotationDeclaration {
    ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
    ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum AnnotationDeclarationShort {
    ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple,
    ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple,
}

#[derive(Debug, Clone)]
pub enum AnyIdentifier {
    PtrIdentifier,
    ReservedKeyword,
}

#[derive(Debug, Clone)]
pub enum AnyNodeName {
    AnyIdentifier,
}

#[derive(Debug, Clone)]
pub enum AtomicExpr {
    AtomicPath,
    BaseAtomicExpr,
    LANGBRACKET_FullTypeExpr_RANGBRACKET_AtomicExpr_P_TYPECAST,
}

#[derive(Debug, Clone)]
pub enum AtomicPath {
    AtomicExpr_PathStep_P_DOT,
}

#[derive(Debug, Clone)]
pub enum BaseAtomicExpr {
    Collection,
    Constant,
    DUNDERDEFAULT,
    DUNDERNEW,
    DUNDEROLD,
    DUNDERSOURCE,
    DUNDERSPECIFIED,
    DUNDERSUBJECT,
    FreeShape,
    FuncExpr,
    NamedTuple,
    NodeName_P_DOT,
    ParenExpr_P_UMINUS,
    PathStep_P_DOT,
    Set,
    Tuple,
}

#[derive(Debug, Clone)]
pub enum BaseBooleanConstant {
    FALSE,
    TRUE,
}

#[derive(Debug, Clone)]
pub enum BaseBytesConstant {
    BCONST,
}

#[derive(Debug, Clone)]
pub enum BaseName {
    Identifier,
    QualifiedName,
}

#[derive(Debug, Clone)]
pub enum BaseNumberConstant {
    FCONST,
    ICONST,
    NFCONST,
    NICONST,
}

#[derive(Debug, Clone)]
pub enum BaseStringConstant {
    SCONST,
}

#[derive(Debug, Clone)]
pub enum BranchOptions {
    FORCE,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum BranchStmt {
    AlterBranchStmt,
    CreateBranchStmt,
    DropBranchStmt,
}

#[derive(Debug, Clone)]
pub enum ByClause {
    BY_GroupingElementList,
}

#[derive(Debug, Clone)]
pub enum CastAllowedUse {
    ALLOW_ASSIGNMENT,
    ALLOW_IMPLICIT,
}

#[derive(Debug, Clone)]
pub enum CastCode {
    USING_Identifier_BaseStringConstant,
    USING_Identifier_CAST,
    USING_Identifier_EXPRESSION,
    USING_Identifier_FUNCTION_BaseStringConstant,
}

#[derive(Debug, Clone)]
pub enum Collection {
    LBRACKET_OptExprList_RBRACKET,
}

#[derive(Debug, Clone)]
pub enum CollectionTypeName {
    NodeName_LANGBRACKET_RANGBRACKET,
    NodeName_LANGBRACKET_SubtypeList_RANGBRACKET,
}

#[derive(Debug, Clone)]
pub enum ColonedIdents {
    AnyIdentifier,
    ColonedIdents_DOUBLECOLON_AnyIdentifier,
}

#[derive(Debug, Clone)]
pub enum CommitMigrationStmt {
    COMMIT_MIGRATION,
    COMMIT_MIGRATION_REWRITE,
}

#[derive(Debug, Clone)]
pub enum CompareOp {
    DISTINCTFROM_P_COMPARE_OP,
    EQUALS_P_COMPARE_OP,
    GREATEREQ_P_COMPARE_OP,
    LANGBRACKET_P_COMPARE_OP,
    LESSEQ_P_COMPARE_OP,
    NOTDISTINCTFROM_P_COMPARE_OP,
    NOTEQ_P_COMPARE_OP,
    RANGBRACKET_P_COMPARE_OP,
}

#[derive(Debug, Clone)]
pub enum ComputableShapePointer {
    MULTI_SimpleShapePointer_ASSIGN_Expr,
    OPTIONAL_MULTI_SimpleShapePointer_ASSIGN_Expr,
    OPTIONAL_SINGLE_SimpleShapePointer_ASSIGN_Expr,
    OPTIONAL_SimpleShapePointer_ASSIGN_Expr,
    REQUIRED_MULTI_SimpleShapePointer_ASSIGN_Expr,
    REQUIRED_SINGLE_SimpleShapePointer_ASSIGN_Expr,
    REQUIRED_SimpleShapePointer_ASSIGN_Expr,
    SINGLE_SimpleShapePointer_ASSIGN_Expr,
    SimpleShapePointer_ADDASSIGN_Expr,
    SimpleShapePointer_ASSIGN_Expr,
    SimpleShapePointer_REMASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum ConcreteConstraintBlock {
    CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
    DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ConcreteConstraintShort {
    CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
    DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
}

#[derive(Debug, Clone)]
pub enum ConcreteIndexDeclarationBlock {
    DEFERRED_INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ConcreteIndexDeclarationShort {
    DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr,
    DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
    INDEX_NodeName_OnExpr_OptExceptExpr,
    INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
    DEFERRED_INDEX_OnExpr_OptExceptExpr,
    INDEX_OnExpr_OptExceptExpr,
}

#[derive(Debug, Clone)]
pub enum ConcreteLinkBlock {
    OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ConcreteLinkShort {
    LINK_PathNodeName_ASSIGN_Expr,
    OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
    OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
    PtrQuals_LINK_PathNodeName_ASSIGN_Expr,
    LINK_PathNodeName_OptExtendingSimple_PtrTarget,
    PtrQuals_LINK_PathNodeName_OptExtendingSimple_PtrTarget,
}

#[derive(Debug, Clone)]
pub enum ConcretePropertyBlock {
    OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
    OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
    PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
    PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ConcretePropertyShort {
    PROPERTY_PathNodeName_ASSIGN_Expr,
    OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
    OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
    PtrQuals_PROPERTY_PathNodeName_ASSIGN_Expr,
    PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
    PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
}

#[derive(Debug, Clone)]
pub enum ConcreteUnknownPointerBlock {
    OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ConcreteUnknownPointerObjectShort {
    PathNodeName_ASSIGN_Expr,
    PtrQuals_PathNodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum ConcreteUnknownPointerShort {
    OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget,
    OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget,
    PathNodeName_OptExtendingSimple_PtrTarget,
    PtrQuals_PathNodeName_OptExtendingSimple_PtrTarget,
}

#[derive(Debug, Clone)]
pub enum ConfigOp {
    INSERT_NodeName_Shape,
    RESET_NodeName_OptFilterClause,
    SET_NodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum ConfigScope {
    CURRENT_BRANCH,
    CURRENT_DATABASE,
    INSTANCE,
    SESSION,
    SYSTEM,
}

#[derive(Debug, Clone)]
pub enum ConfigStmt {
    CONFIGURE_BRANCH_ConfigOp,
    CONFIGURE_ConfigScope_ConfigOp,
    CONFIGURE_DATABASE_ConfigOp,
    RESET_GLOBAL_NodeName,
    SET_GLOBAL_NodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum Constant {
    BaseBooleanConstant,
    BaseBytesConstant,
    BaseNumberConstant,
    BaseStringConstant,
    PARAMETER,
    PARAMETERANDTYPE,
}

#[derive(Debug, Clone)]
pub enum ConstraintDeclaration {
    ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
    ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ConstraintDeclarationShort {
    ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple,
    ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicyCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicyCommandsBlock {
    LBRACE_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicyCommandsList {
    CreateAccessPolicyCommand,
    CreateAccessPolicyCommandsList_Semicolons_CreateAccessPolicyCommand,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicySDLCommandFull {
    CreateAccessPolicySDLCommandBlock,
    CreateAccessPolicySDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicySDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicySDLCommandsBlock {
    LBRACE_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicySDLCommandsList {
    CreateAccessPolicySDLCommandFull,
    CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateAccessPolicyStmt {
    CREATE_ACCESS_POLICY_UnqualifiedPointerName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_OptCreateAccessPolicyCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateAliasCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreateAliasCommandsBlock {
    CreateAliasCommand,
    LBRACE_CreateAliasCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateAliasCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateAliasCommandsList {
    CreateAliasCommand,
    CreateAliasCommandsList_Semicolons_CreateAliasCommand,
}

#[derive(Debug, Clone)]
pub enum CreateAliasSDLCommandFull {
    CreateAliasSDLCommandBlock,
    CreateAliasSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateAliasSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreateAliasSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateAliasSDLCommandsList {
    CreateAliasSDLCommandFull,
    CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateAliasSingleSDLCommandBlock {
    CreateAliasSDLCommandBlock,
    CreateAliasSDLCommandShort,
}

#[derive(Debug, Clone)]
pub enum CreateAliasStmt {
    CREATE_ALIAS_NodeName_CreateAliasCommandsBlock,
    CREATE_ALIAS_NodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum CreateAnnotationCommand {
    CreateAnnotationValueStmt,
}

#[derive(Debug, Clone)]
pub enum CreateAnnotationCommandsBlock {
    LBRACE_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateAnnotationCommandsList {
    CreateAnnotationCommand,
    CreateAnnotationCommandsList_Semicolons_CreateAnnotationCommand,
}

#[derive(Debug, Clone)]
pub enum CreateAnnotationStmt {
    CREATE_ABSTRACT_ANNOTATION_NodeName_OptCreateAnnotationCommandsBlock,
    CREATE_ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptCreateCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateAnnotationValueStmt {
    CREATE_ANNOTATION_NodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum CreateBranchStmt {
    CREATE_EMPTY_BRANCH_DatabaseName,
    CREATE_DATA_BRANCH_DatabaseName_FROM_DatabaseName,
    CREATE_SCHEMA_BRANCH_DatabaseName_FROM_DatabaseName,
    CREATE_TEMPLATE_BRANCH_DatabaseName_FROM_DatabaseName,
}

#[derive(Debug, Clone)]
pub enum CreateCastCommand {
    AlterAnnotationValueStmt,
    CastAllowedUse,
    CastCode,
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateCastCommandsBlock {
    CreateCastCommand,
    LBRACE_CreateCastCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateCastCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateCastCommandsList {
    CreateCastCommand,
    CreateCastCommandsList_Semicolons_CreateCastCommand,
}

#[derive(Debug, Clone)]
pub enum CreateCastStmt {
    CREATE_CAST_FROM_TypeName_TO_TypeName_CreateCastCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreateCommandsBlock {
    LBRACE_CreateCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateCommandsList {
    CreateCommand,
    CreateCommandsList_Semicolons_CreateCommand,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteConstraintStmt {
    CREATE_OptDelegated_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_OptCreateCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteIndexSDLCommandFull {
    CreateConcreteIndexSDLCommandBlock,
    CreateConcreteIndexSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteIndexSDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteIndexSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteIndexSDLCommandsList {
    CreateConcreteIndexSDLCommandFull,
    CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteIndexStmt {
    CREATE_OptDeferred_INDEX_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
    CREATE_OptDeferred_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    CreateConcretePropertyStmt,
    CreateRewriteStmt,
    CreateSimpleExtending,
    OnSourceDeleteStmt,
    OnTargetDeleteStmt,
    SetFieldStmt,
    SetRequiredInCreateStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkCommandsBlock {
    LBRACE_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkCommandsList {
    CreateConcreteLinkCommand,
    CreateConcreteLinkCommandsList_Semicolons_CreateConcreteLinkCommand,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkSDLCommandBlock {
    ConcreteConstraintBlock,
    ConcreteIndexDeclarationBlock,
    ConcretePropertyBlock,
    ConcreteUnknownPointerBlock,
    RewriteDeclarationBlock,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkSDLCommandFull {
    CreateConcreteLinkSDLCommandBlock,
    CreateConcreteLinkSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkSDLCommandShort {
    ConcreteConstraintShort,
    ConcreteIndexDeclarationShort,
    ConcretePropertyShort,
    ConcreteUnknownPointerShort,
    CreateSimpleExtending,
    OnSourceDeleteStmt,
    OnTargetDeleteStmt,
    RewriteDeclarationShort,
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkSDLCommandsList {
    CreateConcreteLinkSDLCommandFull,
    CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateConcreteLinkStmt {
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_ASSIGN_Expr,
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptCreateConcreteLinkCommandsBlock,
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertyCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateRewriteStmt,
    CreateSimpleExtending,
    SetFieldStmt,
    SetRequiredInCreateStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertyCommandsBlock {
    LBRACE_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertyCommandsList {
    CreateConcretePropertyCommand,
    CreateConcretePropertyCommandsList_Semicolons_CreateConcretePropertyCommand,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertySDLCommandBlock {
    ConcreteConstraintBlock,
    RewriteDeclarationBlock,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertySDLCommandFull {
    CreateConcretePropertySDLCommandBlock,
    CreateConcretePropertySDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertySDLCommandShort {
    ConcreteConstraintShort,
    CreateSimpleExtending,
    RewriteDeclarationShort,
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertySDLCommandsBlock {
    LBRACE_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertySDLCommandsList {
    CreateConcretePropertySDLCommandFull,
    CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateConcretePropertyStmt {
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_ASSIGN_Expr,
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptCreateConcretePropertyCommandsBlock,
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateConstraintStmt {
    CREATE_ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
    CREATE_ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateDatabaseCommand {
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateDatabaseCommandsBlock {
    LBRACE_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateDatabaseCommandsList {
    CreateDatabaseCommand,
    CreateDatabaseCommandsList_Semicolons_CreateDatabaseCommand,
}

#[derive(Debug, Clone)]
pub enum CreateDatabaseStmt {
    CREATE_DATABASE_DatabaseName_FROM_AnyNodeName_OptCreateDatabaseCommandsBlock,
    CREATE_DATABASE_DatabaseName_OptCreateDatabaseCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionCommand {
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionCommandsBlock {
    LBRACE_CreateExtensionCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateExtensionCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionCommandsList {
    CreateExtensionCommand,
    CreateExtensionCommandsList_Semicolons_CreateExtensionCommand,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionPackageCommand {
    NestedQLBlockStmt,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionPackageCommandsBlock {
    LBRACE_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionPackageCommandsList {
    CreateExtensionPackageCommand,
    CreateExtensionPackageCommandsList_Semicolons_CreateExtensionPackageCommand,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionPackageStmt {
    CREATE_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion_OptCreateExtensionPackageCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateExtensionStmt {
    CREATE_EXTENSION_ShortNodeName_OptExtensionVersion_OptCreateExtensionCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionArgs {
    LPAREN_FuncDeclArgs_RPAREN,
    LPAREN_RPAREN,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    FromFunction,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionCommandsBlock {
    CreateFunctionCommand,
    LBRACE_CreateFunctionCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateFunctionCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionCommandsList {
    CreateFunctionCommand,
    CreateFunctionCommandsList_Semicolons_CreateFunctionCommand,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionSDLCommandFull {
    CreateFunctionSDLCommandBlock,
    CreateFunctionSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionSDLCommandShort {
    FromFunction,
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionSDLCommandsList {
    CreateFunctionSDLCommandFull,
    CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionSingleSDLCommandBlock {
    CreateFunctionSDLCommandBlock,
    CreateFunctionSDLCommandShort,
}

#[derive(Debug, Clone)]
pub enum CreateFunctionStmt {
    CREATE_FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateFutureStmt {
    CREATE_FUTURE_ShortNodeName,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalCommandsBlock {
    LBRACE_CreateGlobalCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateGlobalCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalCommandsList {
    CreateGlobalCommand,
    CreateGlobalCommandsList_Semicolons_CreateGlobalCommand,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalSDLCommandFull {
    CreateGlobalSDLCommandBlock,
    CreateGlobalSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalSDLCommandsList {
    CreateGlobalSDLCommandFull,
    CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateGlobalStmt {
    CREATE_OptPtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
    CREATE_OptPtrQuals_GLOBAL_NodeName_OptCreateConcretePropertyCommandsBlock,
    CREATE_OptPtrQuals_GLOBAL_NodeName_ARROW_FullTypeExpr_OptCreateGlobalCommandsBlock,
    CREATE_OptPtrQuals_GLOBAL_NodeName_COLON_FullTypeExpr_OptCreateGlobalCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateIndexCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreateIndexCommandsBlock {
    LBRACE_CreateIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateIndexCommandsList {
    CreateIndexCommand,
    CreateIndexCommandsList_Semicolons_CreateIndexCommand,
}

#[derive(Debug, Clone)]
pub enum CreateIndexMatchCommand {
    CreateAnnotationValueStmt,
}

#[derive(Debug, Clone)]
pub enum CreateIndexMatchCommandsBlock {
    LBRACE_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateIndexMatchCommandsList {
    CreateIndexMatchCommand,
    CreateIndexMatchCommandsList_Semicolons_CreateIndexMatchCommand,
}

#[derive(Debug, Clone)]
pub enum CreateIndexMatchStmt {
    CREATE_INDEX_MATCH_FOR_TypeName_USING_NodeName_OptCreateIndexMatchCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateIndexSDLCommandFull {
    CreateIndexSDLCommandBlock,
    CreateIndexSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateIndexSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreateIndexSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateIndexSDLCommandsList {
    CreateIndexSDLCommandFull,
    CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateIndexStmt {
    CREATE_ABSTRACT_INDEX_NodeName_OptExtendingSimple_OptCreateIndexCommandsBlock,
    CREATE_ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_OptCreateIndexCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateLinkCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    CreateConcretePropertyStmt,
    CreateRewriteStmt,
    CreateSimpleExtending,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateLinkCommandsBlock {
    LBRACE_CreateLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateLinkCommandsList {
    CreateLinkCommand,
    CreateLinkCommandsList_Semicolons_CreateLinkCommand,
}

#[derive(Debug, Clone)]
pub enum CreateLinkSDLCommandBlock {
    ConcreteConstraintBlock,
    ConcreteIndexDeclarationBlock,
    ConcretePropertyBlock,
    ConcreteUnknownPointerBlock,
    RewriteDeclarationBlock,
}

#[derive(Debug, Clone)]
pub enum CreateLinkSDLCommandFull {
    CreateLinkSDLCommandBlock,
    CreateLinkSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateLinkSDLCommandShort {
    ConcreteConstraintShort,
    ConcreteIndexDeclarationShort,
    ConcretePropertyShort,
    ConcreteUnknownPointerShort,
    CreateSimpleExtending,
    RewriteDeclarationShort,
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateLinkSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateLinkSDLCommandsList {
    CreateLinkSDLCommandFull,
    CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateLinkStmt {
    CREATE_ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_OptCreateLinkCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateMigrationCommand {
    NestedQLBlockStmt,
}

#[derive(Debug, Clone)]
pub enum CreateMigrationCommandsBlock {
    LBRACE_CreateMigrationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateMigrationCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateMigrationCommandsList {
    CreateMigrationCommand,
    CreateMigrationCommandsList_Semicolons_CreateMigrationCommand,
}

#[derive(Debug, Clone)]
pub enum CreateMigrationStmt {
    CREATE_APPLIED_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
    CREATE_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateModuleStmt {
    CREATE_MODULE_ModuleName_OptIfNotExists_OptCreateCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeCommand {
    AlterAccessPolicyStmt,
    AlterAnnotationValueStmt,
    AlterConcreteConstraintStmt,
    AlterConcreteIndexStmt,
    AlterConcreteLinkStmt,
    AlterConcretePropertyStmt,
    AlterTriggerStmt,
    CreateAccessPolicyStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    CreateConcreteLinkStmt,
    CreateConcretePropertyStmt,
    CreateTriggerStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeCommandsBlock {
    LBRACE_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeCommandsList {
    CreateObjectTypeCommand,
    CreateObjectTypeCommandsList_Semicolons_CreateObjectTypeCommand,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeSDLCommandBlock {
    AccessPolicyDeclarationBlock,
    ConcreteConstraintBlock,
    ConcreteIndexDeclarationBlock,
    ConcreteLinkBlock,
    ConcretePropertyBlock,
    ConcreteUnknownPointerBlock,
    TriggerDeclarationBlock,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeSDLCommandFull {
    CreateObjectTypeSDLCommandBlock,
    CreateObjectTypeSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeSDLCommandShort {
    AccessPolicyDeclarationShort,
    ConcreteConstraintShort,
    ConcreteIndexDeclarationShort,
    ConcreteLinkShort,
    ConcretePropertyShort,
    ConcreteUnknownPointerObjectShort,
    ConcreteUnknownPointerShort,
    SetAnnotation,
    TriggerDeclarationShort,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeSDLCommandsList {
    CreateObjectTypeSDLCommandFull,
    CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateObjectTypeStmt {
    CREATE_ABSTRACT_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
    CREATE_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateOperatorCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    OperatorCode,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateOperatorCommandsBlock {
    CreateOperatorCommand,
    LBRACE_CreateOperatorCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateOperatorCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateOperatorCommandsList {
    CreateOperatorCommand,
    CreateOperatorCommandsList_Semicolons_CreateOperatorCommand,
}

#[derive(Debug, Clone)]
pub enum CreateOperatorStmt {
    CREATE_ABSTRACT_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_OptCreateOperatorCommandsBlock,
    CREATE_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateOperatorCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreatePropertyCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateSimpleExtending,
    SetFieldStmt,
    UsingStmt,
}

#[derive(Debug, Clone)]
pub enum CreatePropertyCommandsBlock {
    LBRACE_CreatePropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreatePropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreatePropertyCommandsList {
    CreatePropertyCommand,
    CreatePropertyCommandsList_Semicolons_CreatePropertyCommand,
}

#[derive(Debug, Clone)]
pub enum CreatePropertySDLCommandFull {
    CreatePropertySDLCommandBlock,
    CreatePropertySDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreatePropertySDLCommandShort {
    CreateSimpleExtending,
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreatePropertySDLCommandsBlock {
    LBRACE_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreatePropertySDLCommandsList {
    CreatePropertySDLCommandFull,
    CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreatePropertyStmt {
    CREATE_ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_OptCreatePropertyCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreatePseudoTypeCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreatePseudoTypeCommandsBlock {
    LBRACE_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreatePseudoTypeCommandsList {
    CreatePseudoTypeCommand,
    CreatePseudoTypeCommandsList_Semicolons_CreatePseudoTypeCommand,
}

#[derive(Debug, Clone)]
pub enum CreatePseudoTypeStmt {
    CREATE_PSEUDO_TYPE_NodeName_OptCreatePseudoTypeCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteCommandsBlock {
    LBRACE_CreateRewriteCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateRewriteCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteCommandsList {
    CreateRewriteCommand,
    CreateRewriteCommandsList_Semicolons_CreateRewriteCommand,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteSDLCommandFull {
    CreateRewriteSDLCommandBlock,
    CreateRewriteSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteSDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteSDLCommandsList {
    CreateRewriteSDLCommandFull,
    CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateRewriteStmt {
    CREATE_REWRITE_RewriteKindList_USING_ParenExpr_OptCreateRewriteCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateRoleCommand {
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateRoleCommandsBlock {
    LBRACE_CreateRoleCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateRoleCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateRoleCommandsList {
    CreateRoleCommand,
    CreateRoleCommandsList_Semicolons_CreateRoleCommand,
}

#[derive(Debug, Clone)]
pub enum CreateRoleStmt {
    CREATE_OptSuperuser_ROLE_ShortNodeName_OptShortExtending_OptIfNotExists_OptCreateRoleCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateSDLCommandFull {
    CreateSDLCommandBlock,
    CreateSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(Debug, Clone)]
pub enum CreateSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_CreateSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateSDLCommandsList {
    CreateSDLCommandFull,
    CreateSDLCommandsList_OptSemicolons_CreateSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeCommandsBlock {
    LBRACE_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeCommandsList {
    CreateScalarTypeCommand,
    CreateScalarTypeCommandsList_Semicolons_CreateScalarTypeCommand,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeSDLCommandBlock {
    ConcreteConstraintBlock,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeSDLCommandFull {
    CreateScalarTypeSDLCommandBlock,
    CreateScalarTypeSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeSDLCommandShort {
    ConcreteConstraintShort,
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeSDLCommandsList {
    CreateScalarTypeSDLCommandFull,
    CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateScalarTypeStmt {
    CREATE_ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
    CREATE_FINAL_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
    CREATE_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum CreateSimpleExtending {
    EXTENDING_SimpleTypeNameList,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerCommandsBlock {
    LBRACE_CreateTriggerCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateTriggerCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerCommandsList {
    CreateTriggerCommand,
    CreateTriggerCommandsList_Semicolons_CreateTriggerCommand,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerSDLCommandFull {
    CreateTriggerSDLCommandBlock,
    CreateTriggerSDLCommandShort_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerSDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerSDLCommandsList {
    CreateTriggerSDLCommandFull,
    CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandFull,
}

#[derive(Debug, Clone)]
pub enum CreateTriggerStmt {
    CREATE_TRIGGER_UnqualifiedPointerName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_OptCreateTriggerCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum DDLStmt {
    BranchStmt,
    DatabaseStmt,
    ExtensionPackageStmt,
    MigrationStmt,
    OptWithDDLStmt,
    RoleStmt,
}

#[derive(Debug, Clone)]
pub enum DDLWithBlock {
    WithBlock,
}

#[derive(Debug, Clone)]
pub enum DatabaseName {
    Identifier,
    ReservedKeyword,
}

#[derive(Debug, Clone)]
pub enum DatabaseStmt {
    AlterDatabaseStmt,
    CreateDatabaseStmt,
    DropDatabaseStmt,
}

#[derive(Debug, Clone)]
pub enum DescribeFormat {
    AS_DDL,
    AS_JSON,
    AS_SDL,
    AS_TEXT,
    AS_TEXT_VERBOSE,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum DescribeStmt {
    DESCRIBE_CURRENT_BRANCH_CONFIG_DescribeFormat,
    DESCRIBE_CURRENT_DATABASE_CONFIG_DescribeFormat,
    DESCRIBE_CURRENT_MIGRATION_DescribeFormat,
    DESCRIBE_INSTANCE_CONFIG_DescribeFormat,
    DESCRIBE_OBJECT_NodeName_DescribeFormat,
    DESCRIBE_ROLES_DescribeFormat,
    DESCRIBE_SCHEMA_DescribeFormat,
    DESCRIBE_SYSTEM_CONFIG_DescribeFormat,
    DESCRIBE_SchemaItem_DescribeFormat,
}

#[derive(Debug, Clone)]
pub enum DotName {
    DottedIdents,
}

#[derive(Debug, Clone)]
pub enum DottedIdents {
    AnyIdentifier,
    DottedIdents_DOT_AnyIdentifier,
}

#[derive(Debug, Clone)]
pub enum DropAccessPolicyStmt {
    DROP_ACCESS_POLICY_UnqualifiedPointerName,
}

#[derive(Debug, Clone)]
pub enum DropAliasStmt {
    DROP_ALIAS_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropAnnotationStmt {
    DROP_ABSTRACT_ANNOTATION_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropAnnotationValueStmt {
    DROP_ANNOTATION_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropBranchStmt {
    DROP_BRANCH_DatabaseName_BranchOptions,
}

#[derive(Debug, Clone)]
pub enum DropCastStmt {
    DROP_CAST_FROM_TypeName_TO_TypeName,
}

#[derive(Debug, Clone)]
pub enum DropConcreteConstraintStmt {
    DROP_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
}

#[derive(Debug, Clone)]
pub enum DropConcreteIndexCommand {
    SetFieldStmt,
}

#[derive(Debug, Clone)]
pub enum DropConcreteIndexCommandsBlock {
    LBRACE_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum DropConcreteIndexCommandsList {
    DropConcreteIndexCommand,
    DropConcreteIndexCommandsList_Semicolons_DropConcreteIndexCommand,
}

#[derive(Debug, Clone)]
pub enum DropConcreteIndexStmt {
    DROP_INDEX_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
    DROP_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum DropConcreteLinkCommand {
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcretePropertyStmt,
}

#[derive(Debug, Clone)]
pub enum DropConcreteLinkCommandsBlock {
    LBRACE_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum DropConcreteLinkCommandsList {
    DropConcreteLinkCommand,
    DropConcreteLinkCommandsList_Semicolons_DropConcreteLinkCommand,
}

#[derive(Debug, Clone)]
pub enum DropConcreteLinkStmt {
    DROP_LINK_UnqualifiedPointerName_OptDropConcreteLinkCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum DropConcretePropertyStmt {
    DROP_PROPERTY_UnqualifiedPointerName,
}

#[derive(Debug, Clone)]
pub enum DropConstraintStmt {
    DROP_ABSTRACT_CONSTRAINT_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropDatabaseStmt {
    DROP_DATABASE_DatabaseName,
}

#[derive(Debug, Clone)]
pub enum DropExtensionPackageStmt {
    DROP_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion,
}

#[derive(Debug, Clone)]
pub enum DropExtensionStmt {
    DROP_EXTENSION_ShortNodeName_OptExtensionVersion,
}

#[derive(Debug, Clone)]
pub enum DropFunctionStmt {
    DROP_FUNCTION_NodeName_CreateFunctionArgs,
}

#[derive(Debug, Clone)]
pub enum DropFutureStmt {
    DROP_FUTURE_ShortNodeName,
}

#[derive(Debug, Clone)]
pub enum DropGlobalStmt {
    DROP_GLOBAL_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropIndexMatchStmt {
    DROP_INDEX_MATCH_FOR_TypeName_USING_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropIndexStmt {
    DROP_ABSTRACT_INDEX_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropLinkCommand {
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcretePropertyStmt,
}

#[derive(Debug, Clone)]
pub enum DropLinkCommandsBlock {
    LBRACE_DropLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum DropLinkCommandsList {
    DropLinkCommand,
    DropLinkCommandsList_Semicolons_DropLinkCommand,
}

#[derive(Debug, Clone)]
pub enum DropLinkStmt {
    DROP_ABSTRACT_LINK_PtrNodeName_OptDropLinkCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum DropMigrationStmt {
    DROP_MIGRATION_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropModuleStmt {
    DROP_MODULE_ModuleName,
}

#[derive(Debug, Clone)]
pub enum DropObjectTypeCommand {
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcreteLinkStmt,
    DropConcretePropertyStmt,
}

#[derive(Debug, Clone)]
pub enum DropObjectTypeCommandsBlock {
    LBRACE_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum DropObjectTypeCommandsList {
    DropObjectTypeCommand,
    DropObjectTypeCommandsList_Semicolons_DropObjectTypeCommand,
}

#[derive(Debug, Clone)]
pub enum DropObjectTypeStmt {
    DROP_TYPE_NodeName_OptDropObjectTypeCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum DropOperatorStmt {
    DROP_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs,
}

#[derive(Debug, Clone)]
pub enum DropPropertyStmt {
    DROP_ABSTRACT_PROPERTY_PtrNodeName,
}

#[derive(Debug, Clone)]
pub enum DropRewriteStmt {
    DROP_REWRITE_RewriteKindList,
}

#[derive(Debug, Clone)]
pub enum DropRoleStmt {
    DROP_ROLE_ShortNodeName,
}

#[derive(Debug, Clone)]
pub enum DropScalarTypeStmt {
    DROP_SCALAR_TYPE_NodeName,
}

#[derive(Debug, Clone)]
pub enum DropTriggerStmt {
    DROP_TRIGGER_UnqualifiedPointerName,
}

#[derive(Debug, Clone)]
pub enum EdgeQLBlock {
    OptSemicolons,
    StatementBlock_OptSemicolons,
}

#[derive(Debug, Clone)]
pub enum EdgeQLGrammar {
    STARTBLOCK_EdgeQLBlock_EOI,
    STARTEXTENSION_CreateExtensionPackageCommandsBlock_EOI,
    STARTFRAGMENT_ExprStmt_EOI,
    STARTFRAGMENT_Expr_EOI,
    STARTMIGRATION_CreateMigrationCommandsBlock_EOI,
    STARTSDLDOCUMENT_SDLDocument_EOI,
}

#[derive(Debug, Clone)]
pub enum Expr {
    BaseAtomicExpr,
    DETACHED_Expr,
    DISTINCT_Expr,
    EXISTS_Expr,
    Expr_AND_Expr,
    Expr_CIRCUMFLEX_Expr,
    Expr_CompareOp_Expr_P_COMPARE_OP,
    Expr_DOUBLEPLUS_Expr,
    Expr_DOUBLEQMARK_Expr_P_DOUBLEQMARK_OP,
    Expr_DOUBLESLASH_Expr,
    Expr_EXCEPT_Expr,
    Expr_IF_Expr_ELSE_Expr,
    Expr_ILIKE_Expr,
    Expr_INTERSECT_Expr,
    Expr_IN_Expr,
    Expr_IS_NOT_TypeExpr_P_IS,
    Expr_IS_TypeExpr,
    Expr_IndirectionEl,
    Expr_LIKE_Expr,
    Expr_MINUS_Expr,
    Expr_NOT_ILIKE_Expr,
    Expr_NOT_IN_Expr_P_IN,
    Expr_NOT_LIKE_Expr,
    Expr_OR_Expr,
    Expr_PERCENT_Expr,
    Expr_PLUS_Expr,
    Expr_SLASH_Expr,
    Expr_STAR_Expr,
    Expr_Shape,
    Expr_UNION_Expr,
    GLOBAL_NodeName,
    INTROSPECT_TypeExpr,
    IfThenElseExpr,
    LANGBRACKET_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
    LANGBRACKET_OPTIONAL_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
    LANGBRACKET_REQUIRED_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
    MINUS_Expr_P_UMINUS,
    NOT_Expr,
    PLUS_Expr_P_UMINUS,
    Path,
}

#[derive(Debug, Clone)]
pub enum ExprList {
    ExprListInner,
    ExprListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum ExprListInner {
    Expr,
    ExprListInner_COMMA_Expr,
}

#[derive(Debug, Clone)]
pub enum ExprStmt {
    ExprStmtCore,
    WithBlock_ExprStmtCore,
}

#[derive(Debug, Clone)]
pub enum ExprStmtCore {
    InternalGroup,
    SimpleDelete,
    SimpleFor,
    SimpleGroup,
    SimpleInsert,
    SimpleSelect,
    SimpleUpdate,
}

#[derive(Debug, Clone)]
pub enum Extending {
    EXTENDING_TypeNameList,
}

#[derive(Debug, Clone)]
pub enum ExtendingSimple {
    EXTENDING_SimpleTypeNameList,
}

#[derive(Debug, Clone)]
pub enum ExtensionPackageStmt {
    CreateExtensionPackageStmt,
    DropExtensionPackageStmt,
}

#[derive(Debug, Clone)]
pub enum ExtensionRequirementDeclaration {
    USING_EXTENSION_ShortNodeName_OptExtensionVersion,
}

#[derive(Debug, Clone)]
pub enum ExtensionStmt {
    CreateExtensionStmt,
    DropExtensionStmt,
}

#[derive(Debug, Clone)]
pub enum ExtensionVersion {
    VERSION_BaseStringConstant,
}

#[derive(Debug, Clone)]
pub enum FilterClause {
    FILTER_Expr,
}

#[derive(Debug, Clone)]
pub enum FreeComputableShapePointer {
    FreeSimpleShapePointer_ASSIGN_Expr,
    MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
    OPTIONAL_FreeSimpleShapePointer_ASSIGN_Expr,
    OPTIONAL_MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
    OPTIONAL_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
    REQUIRED_FreeSimpleShapePointer_ASSIGN_Expr,
    REQUIRED_MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
    REQUIRED_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
    SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum FreeComputableShapePointerList {
    FreeComputableShapePointerListInner,
    FreeComputableShapePointerListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum FreeComputableShapePointerListInner {
    FreeComputableShapePointer,
    FreeComputableShapePointerListInner_COMMA_FreeComputableShapePointer,
}

#[derive(Debug, Clone)]
pub enum FreeShape {
    LBRACE_FreeComputableShapePointerList_RBRACE,
}

#[derive(Debug, Clone)]
pub enum FreeSimpleShapePointer {
    FreeStepName,
}

#[derive(Debug, Clone)]
pub enum FreeStepName {
    DUNDERTYPE,
    ShortNodeName,
}

#[derive(Debug, Clone)]
pub enum FromFunction {
    USING_Identifier_BaseStringConstant,
    USING_Identifier_EXPRESSION,
    USING_Identifier_FUNCTION_BaseStringConstant,
    USING_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum FullTypeExpr {
    FullTypeExpr_AMPER_FullTypeExpr,
    FullTypeExpr_PIPE_FullTypeExpr,
    LPAREN_FullTypeExpr_RPAREN,
    TYPEOF_Expr,
    TypeName,
}

#[derive(Debug, Clone)]
pub enum FuncApplication {
    NodeName_LPAREN_OptFuncArgList_RPAREN,
}

#[derive(Debug, Clone)]
pub enum FuncArgList {
    FuncArgListInner,
    FuncArgListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum FuncArgListInner {
    FuncArgListInner_COMMA_FuncCallArg,
    FuncCallArg,
}

#[derive(Debug, Clone)]
pub enum FuncCallArg {
    FuncCallArgExpr_OptFilterClause_OptSortClause,
}

#[derive(Debug, Clone)]
pub enum FuncCallArgExpr {
    AnyIdentifier_ASSIGN_Expr,
    Expr,
    PARAMETER_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum FuncDeclArg {
    OptParameterKind_FuncDeclArgName_OptDefault,
    OptParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
}

#[derive(Debug, Clone)]
pub enum FuncDeclArgList {
    FuncDeclArgListInner,
    FuncDeclArgListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum FuncDeclArgListInner {
    FuncDeclArg,
    FuncDeclArgListInner_COMMA_FuncDeclArg,
}

#[derive(Debug, Clone)]
pub enum FuncDeclArgName {
    Identifier,
    PARAMETER,
}

#[derive(Debug, Clone)]
pub enum FuncDeclArgs {
    FuncDeclArgList,
}

#[derive(Debug, Clone)]
pub enum FuncExpr {
    FuncApplication,
}

#[derive(Debug, Clone)]
pub enum FunctionDeclaration {
    FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum FunctionDeclarationShort {
    FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSingleSDLCommandBlock,
}

#[derive(Debug, Clone)]
pub enum FunctionType {
    FullTypeExpr,
}

#[derive(Debug, Clone)]
pub enum FutureRequirementDeclaration {
    USING_FUTURE_ShortNodeName,
}

#[derive(Debug, Clone)]
pub enum FutureStmt {
    CreateFutureStmt,
    DropFutureStmt,
}

#[derive(Debug, Clone)]
pub enum GlobalDeclaration {
    GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
    PtrQuals_GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum GlobalDeclarationShort {
    GLOBAL_NodeName_ASSIGN_Expr,
    PtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
    GLOBAL_NodeName_PtrTarget,
    PtrQuals_GLOBAL_NodeName_PtrTarget,
}

#[derive(Debug, Clone)]
pub enum GroupingAtom {
    GroupingIdent,
    LPAREN_GroupingIdentList_RPAREN,
}

#[derive(Debug, Clone)]
pub enum GroupingAtomList {
    GroupingAtomListInner,
    GroupingAtomListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum GroupingAtomListInner {
    GroupingAtom,
    GroupingAtomListInner_COMMA_GroupingAtom,
}

#[derive(Debug, Clone)]
pub enum GroupingElement {
    CUBE_LPAREN_GroupingAtomList_RPAREN,
    GroupingAtom,
    LBRACE_GroupingElementList_RBRACE,
    ROLLUP_LPAREN_GroupingAtomList_RPAREN,
}

#[derive(Debug, Clone)]
pub enum GroupingElementList {
    GroupingElementListInner,
    GroupingElementListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum GroupingElementListInner {
    GroupingElement,
    GroupingElementListInner_COMMA_GroupingElement,
}

#[derive(Debug, Clone)]
pub enum GroupingIdent {
    AT_Identifier,
    DOT_Identifier,
    Identifier,
}

#[derive(Debug, Clone)]
pub enum GroupingIdentList {
    GroupingIdent,
    GroupingIdentList_COMMA_GroupingIdent,
}

#[derive(Debug, Clone)]
pub enum Identifier {
    IDENT,
    UnreservedKeyword,
}

#[derive(Debug, Clone)]
pub enum IfThenElseExpr {
    IF_Expr_THEN_Expr_ELSE_Expr,
}

#[derive(Debug, Clone)]
pub enum IndexArg {
    AnyIdentifier_ASSIGN_Expr,
    FuncDeclArgName_OptDefault,
    FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
    ParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
}

#[derive(Debug, Clone)]
pub enum IndexArgList {
    IndexArgListInner,
    IndexArgListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum IndexArgListInner {
    IndexArg,
    IndexArgListInner_COMMA_IndexArg,
}

#[derive(Debug, Clone)]
pub enum IndexDeclaration {
    ABSTRACT_INDEX_NodeName_OptExtendingSimple_CreateIndexSDLCommandsBlock,
    ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_CreateIndexSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum IndexDeclarationShort {
    ABSTRACT_INDEX_NodeName_OptExtendingSimple,
    ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple,
}

#[derive(Debug, Clone)]
pub enum IndexExtArgList {
    LPAREN_OptIndexArgList_RPAREN,
}

#[derive(Debug, Clone)]
pub enum IndirectionEl {
    LBRACKET_COLON_Expr_RBRACKET,
    LBRACKET_Expr_COLON_Expr_RBRACKET,
    LBRACKET_Expr_COLON_RBRACKET,
    LBRACKET_Expr_RBRACKET,
}

#[derive(Debug, Clone)]
pub enum InnerDDLStmt {
    AlterAliasStmt,
    AlterAnnotationStmt,
    AlterCastStmt,
    AlterConstraintStmt,
    AlterFunctionStmt,
    AlterGlobalStmt,
    AlterIndexStmt,
    AlterLinkStmt,
    AlterModuleStmt,
    AlterObjectTypeStmt,
    AlterOperatorStmt,
    AlterPropertyStmt,
    AlterScalarTypeStmt,
    CreateAliasStmt,
    CreateAnnotationStmt,
    CreateCastStmt,
    CreateConstraintStmt,
    CreateFunctionStmt,
    CreateGlobalStmt,
    CreateIndexMatchStmt,
    CreateIndexStmt,
    CreateLinkStmt,
    CreateModuleStmt,
    CreateObjectTypeStmt,
    CreateOperatorStmt,
    CreatePropertyStmt,
    CreatePseudoTypeStmt,
    CreateScalarTypeStmt,
    DropAliasStmt,
    DropAnnotationStmt,
    DropCastStmt,
    DropConstraintStmt,
    DropFunctionStmt,
    DropGlobalStmt,
    DropIndexMatchStmt,
    DropIndexStmt,
    DropLinkStmt,
    DropModuleStmt,
    DropObjectTypeStmt,
    DropOperatorStmt,
    DropPropertyStmt,
    DropScalarTypeStmt,
    ExtensionStmt,
    FutureStmt,
}

#[derive(Debug, Clone)]
pub enum InternalGroup {
    FOR_GROUP_OptionallyAliasedExpr_UsingClause_ByClause_IN_Identifier_OptGroupingAlias_UNION_OptionallyAliasedExpr_OptFilterClause_OptSortClause,
}

#[derive(Debug, Clone)]
pub enum LimitClause {
    LIMIT_Expr,
}

#[derive(Debug, Clone)]
pub enum LinkDeclaration {
    ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_CreateLinkSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum LinkDeclarationShort {
    ABSTRACT_LINK_PtrNodeName_OptExtendingSimple,
}

#[derive(Debug, Clone)]
pub enum MigrationStmt {
    AbortMigrationStmt,
    AlterCurrentMigrationStmt,
    AlterMigrationStmt,
    CommitMigrationStmt,
    CreateMigrationStmt,
    DropMigrationStmt,
    PopulateMigrationStmt,
    ResetSchemaStmt,
    StartMigrationStmt,
}

#[derive(Debug, Clone)]
pub enum ModuleDeclaration {
    MODULE_ModuleName_SDLCommandBlock,
}

#[derive(Debug, Clone)]
pub enum ModuleName {
    DotName,
    ModuleName_DOUBLECOLON_DotName,
}

#[derive(Debug, Clone)]
pub enum NamedTuple {
    LPAREN_NamedTupleElementList_RPAREN,
}

#[derive(Debug, Clone)]
pub enum NamedTupleElement {
    ShortNodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum NamedTupleElementList {
    NamedTupleElementListInner,
    NamedTupleElementListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum NamedTupleElementListInner {
    NamedTupleElement,
    NamedTupleElementListInner_COMMA_NamedTupleElement,
}

#[derive(Debug, Clone)]
pub enum NestedQLBlockStmt {
    OptWithDDLStmt,
    SetFieldStmt,
    Stmt,
}

#[derive(Debug, Clone)]
pub enum NodeName {
    BaseName,
}

#[derive(Debug, Clone)]
pub enum NontrivialTypeExpr {
    LPAREN_FullTypeExpr_RPAREN,
    TYPEOF_Expr,
    TypeExpr_AMPER_TypeExpr,
    TypeExpr_PIPE_TypeExpr,
}

#[derive(Debug, Clone)]
pub enum ObjectTypeDeclaration {
    ABSTRACT_TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
    TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ObjectTypeDeclarationShort {
    ABSTRACT_TYPE_NodeName_OptExtendingSimple,
    TYPE_NodeName_OptExtendingSimple,
}

#[derive(Debug, Clone)]
pub enum OffsetClause {
    OFFSET_Expr,
}

#[derive(Debug, Clone)]
pub enum OnExpr {
    ON_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum OnSourceDeleteResetStmt {
    RESET_ON_SOURCE_DELETE,
}

#[derive(Debug, Clone)]
pub enum OnSourceDeleteStmt {
    ON_SOURCE_DELETE_ALLOW,
    ON_SOURCE_DELETE_DELETE_TARGET,
    ON_SOURCE_DELETE_DELETE_TARGET_IF_ORPHAN,
}

#[derive(Debug, Clone)]
pub enum OnTargetDeleteResetStmt {
    RESET_ON_TARGET_DELETE,
}

#[derive(Debug, Clone)]
pub enum OnTargetDeleteStmt {
    ON_TARGET_DELETE_ALLOW,
    ON_TARGET_DELETE_DEFERRED_RESTRICT,
    ON_TARGET_DELETE_DELETE_SOURCE,
    ON_TARGET_DELETE_RESTRICT,
}

#[derive(Debug, Clone)]
pub enum OperatorCode {
    USING_Identifier_BaseStringConstant,
    USING_Identifier_EXPRESSION,
    USING_Identifier_FUNCTION_BaseStringConstant,
    USING_Identifier_OPERATOR_BaseStringConstant,
}

#[derive(Debug, Clone)]
pub enum OperatorKind {
    INFIX,
    POSTFIX,
    PREFIX,
    TERNARY,
}

#[derive(Debug, Clone)]
pub enum OptAlterUsingClause {
    USING_ParenExpr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptAnySubShape {
    COLON_Shape,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptConcreteConstraintArgList {
    LPAREN_OptPosCallArgList_RPAREN,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateAccessPolicyCommandsBlock {
    CreateAccessPolicyCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateAnnotationCommandsBlock {
    CreateAnnotationCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateCommandsBlock {
    CreateCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateConcreteLinkCommandsBlock {
    CreateConcreteLinkCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateConcretePropertyCommandsBlock {
    CreateConcretePropertyCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateDatabaseCommandsBlock {
    CreateDatabaseCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateExtensionCommandsBlock {
    CreateExtensionCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateExtensionPackageCommandsBlock {
    CreateExtensionPackageCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateGlobalCommandsBlock {
    CreateGlobalCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateIndexCommandsBlock {
    CreateIndexCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateIndexMatchCommandsBlock {
    CreateIndexMatchCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateLinkCommandsBlock {
    CreateLinkCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateMigrationCommandsBlock {
    CreateMigrationCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateObjectTypeCommandsBlock {
    CreateObjectTypeCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateOperatorCommandsBlock {
    CreateOperatorCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreatePropertyCommandsBlock {
    CreatePropertyCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreatePseudoTypeCommandsBlock {
    CreatePseudoTypeCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateRewriteCommandsBlock {
    CreateRewriteCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateRoleCommandsBlock {
    CreateRoleCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateScalarTypeCommandsBlock {
    CreateScalarTypeCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptCreateTriggerCommandsBlock {
    CreateTriggerCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDefault {
    EQUALS_Expr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDeferred {
    DEFERRED,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDelegated {
    DELEGATED,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDirection {
    ASC,
    DESC,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDropConcreteIndexCommandsBlock {
    DropConcreteIndexCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDropConcreteLinkCommandsBlock {
    DropConcreteLinkCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDropLinkCommandsBlock {
    DropLinkCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptDropObjectTypeCommandsBlock {
    DropObjectTypeCommandsBlock,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptExceptExpr {
    EXCEPT_ParenExpr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptExprList {
    ExprList,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptExtending {
    Extending,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptExtendingSimple {
    ExtendingSimple,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptExtensionVersion {
    ExtensionVersion,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptFilterClause {
    FilterClause,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptFuncArgList {
    FuncArgList,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptGroupingAlias {
    COMMA_Identifier,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptIfNotExists {
    IF_NOT_EXISTS,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptIndexArgList {
    IndexArgList,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptIndexExtArgList {
    IndexExtArgList,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptMigrationNameParentName {
    ShortNodeName,
    ShortNodeName_ONTO_ShortNodeName,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptNonesOrder {
    EMPTY_FIRST,
    EMPTY_LAST,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptOnExpr {
    OnExpr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptParameterKind {
    ParameterKind,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptPosCallArgList {
    PosCallArgList,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptPosition {
    AFTER_NodeName,
    BEFORE_NodeName,
    FIRST,
    LAST,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptPtrQuals {
    PtrQuals,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptPtrTarget {
    PtrTarget,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptSelectLimit {
    SelectLimit,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptSemicolons {
    Semicolons,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptShortExtending {
    ShortExtending,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptSortClause {
    SortClause,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptSuperuser {
    SUPERUSER,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptTransactionModeList {
    TransactionModeList,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptTypeIntersection {
    TypeIntersection,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptTypeQualifier {
    OPTIONAL,
    SET_OF,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptUnlessConflictClause {
    UnlessConflictCause,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptUsingBlock {
    USING_ParenExpr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptUsingClause {
    UsingClause,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptWhenBlock {
    WHEN_ParenExpr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptWithDDLStmt {
    DDLWithBlock_WithDDLStmt,
    WithDDLStmt,
}

#[derive(Debug, Clone)]
pub enum OptionalOptional {
    OPTIONAL,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum OptionallyAliasedExpr {
    AliasedExpr,
    Expr,
}

#[derive(Debug, Clone)]
pub enum OrderbyExpr {
    Expr_OptDirection_OptNonesOrder,
}

#[derive(Debug, Clone)]
pub enum OrderbyList {
    OrderbyExpr,
    OrderbyList_THEN_OrderbyExpr,
}

#[derive(Debug, Clone)]
pub enum ParameterKind {
    NAMEDONLY,
    VARIADIC,
}

#[derive(Debug, Clone)]
pub enum ParenExpr {
    LPAREN_ExprStmt_RPAREN,
    LPAREN_Expr_RPAREN,
}

#[derive(Debug, Clone)]
pub enum ParenTypeExpr {
    LPAREN_FullTypeExpr_RPAREN,
}

#[derive(Debug, Clone)]
pub enum PartialReservedKeyword {
    EXCEPT,
    INTERSECT,
    UNION,
}

#[derive(Debug, Clone)]
pub enum Path {
    Expr_PathStep_P_DOT,
}

#[derive(Debug, Clone)]
pub enum PathNodeName {
    PtrIdentifier,
}

#[derive(Debug, Clone)]
pub enum PathStep {
    AT_PathNodeName,
    DOTBW_PathStepName,
    DOT_ICONST,
    DOT_PathStepName,
    TypeIntersection,
}

#[derive(Debug, Clone)]
pub enum PathStepName {
    DUNDERTYPE,
    PathNodeName,
}

#[derive(Debug, Clone)]
pub enum PointerName {
    DUNDERTYPE,
    PtrNodeName,
}

#[derive(Debug, Clone)]
pub enum PopulateMigrationStmt {
    POPULATE_MIGRATION,
}

#[derive(Debug, Clone)]
pub enum PosCallArg {
    Expr_OptFilterClause_OptSortClause,
}

#[derive(Debug, Clone)]
pub enum PosCallArgList {
    PosCallArg,
    PosCallArgList_COMMA_PosCallArg,
}

#[derive(Debug, Clone)]
pub enum PropertyDeclaration {
    ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_CreatePropertySDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum PropertyDeclarationShort {
    ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple,
}

#[derive(Debug, Clone)]
pub enum PtrIdentifier {
    Identifier,
    PartialReservedKeyword,
}

#[derive(Debug, Clone)]
pub enum PtrName {
    PtrIdentifier,
    QualifiedName,
}

#[derive(Debug, Clone)]
pub enum PtrNodeName {
    PtrName,
}

#[derive(Debug, Clone)]
pub enum PtrQualifiedNodeName {
    QualifiedName,
}

#[derive(Debug, Clone)]
pub enum PtrQuals {
    MULTI,
    OPTIONAL,
    OPTIONAL_MULTI,
    OPTIONAL_SINGLE,
    REQUIRED,
    REQUIRED_MULTI,
    REQUIRED_SINGLE,
    SINGLE,
}

#[derive(Debug, Clone)]
pub enum PtrTarget {
    ARROW_FullTypeExpr,
    COLON_FullTypeExpr,
}

#[derive(Debug, Clone)]
pub enum QualifiedName {
    DUNDERSTD_DOUBLECOLON_ColonedIdents,
    Identifier_DOUBLECOLON_ColonedIdents,
}

#[derive(Debug, Clone)]
pub enum RenameStmt {
    RENAME_TO_NodeName,
}

#[derive(Debug, Clone)]
pub enum ReservedKeyword {
    ADMINISTER,
    ALTER,
    ANALYZE,
    AND,
    ANYARRAY,
    ANYOBJECT,
    ANYTUPLE,
    ANYTYPE,
    BEGIN,
    BY,
    CASE,
    CHECK,
    COMMIT,
    CONFIGURE,
    CREATE,
    DEALLOCATE,
    DELETE,
    DESCRIBE,
    DETACHED,
    DISCARD,
    DISTINCT,
    DO,
    DROP,
    DUNDERDEFAULT,
    DUNDEREDGEDBSYS,
    DUNDEREDGEDBTPL,
    DUNDERNEW,
    DUNDEROLD,
    DUNDERSOURCE,
    DUNDERSPECIFIED,
    DUNDERSTD,
    DUNDERSUBJECT,
    DUNDERTYPE,
    ELSE,
    END,
    EXISTS,
    EXPLAIN,
    EXTENDING,
    FALSE,
    FETCH,
    FILTER,
    FOR,
    GET,
    GLOBAL,
    GRANT,
    GROUP,
    IF,
    ILIKE,
    IMPORT,
    IN,
    INSERT,
    INTROSPECT,
    IS,
    LIKE,
    LIMIT,
    LISTEN,
    LOAD,
    LOCK,
    MATCH,
    MODULE,
    MOVE,
    NEVER,
    NOT,
    NOTIFY,
    OFFSET,
    ON,
    OPTIONAL,
    OR,
    OVER,
    PARTITION,
    PREPARE,
    RAISE,
    REFRESH,
    REVOKE,
    ROLLBACK,
    SELECT,
    SET,
    SINGLE,
    START,
    TRUE,
    TYPEOF,
    UPDATE,
    VARIADIC,
    WHEN,
    WINDOW,
    WITH,
}

#[derive(Debug, Clone)]
pub enum ResetFieldStmt {
    RESET_DEFAULT,
    RESET_IDENT,
}

#[derive(Debug, Clone)]
pub enum ResetSchemaStmt {
    RESET_SCHEMA_TO_NodeName,
}

#[derive(Debug, Clone)]
pub enum ResetStmt {
    RESET_ALIAS_Identifier,
    RESET_ALIAS_STAR,
    RESET_MODULE,
}

#[derive(Debug, Clone)]
pub enum RewriteDeclarationBlock {
    REWRITE_RewriteKindList_USING_ParenExpr_CreateRewriteSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum RewriteDeclarationShort {
    REWRITE_RewriteKindList_USING_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum RewriteKind {
    INSERT,
    UPDATE,
}

#[derive(Debug, Clone)]
pub enum RewriteKindList {
    RewriteKind,
    RewriteKindList_COMMA_RewriteKind,
}

#[derive(Debug, Clone)]
pub enum RoleStmt {
    AlterRoleStmt,
    CreateRoleStmt,
    DropRoleStmt,
}

#[derive(Debug, Clone)]
pub enum SDLBlockStatement {
    AliasDeclaration,
    AnnotationDeclaration,
    ConstraintDeclaration,
    FunctionDeclaration,
    GlobalDeclaration,
    IndexDeclaration,
    LinkDeclaration,
    ModuleDeclaration,
    ObjectTypeDeclaration,
    PropertyDeclaration,
    ScalarTypeDeclaration,
}

#[derive(Debug, Clone)]
pub enum SDLCommandBlock {
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_SDLStatements_RBRACE,
    LBRACE_OptSemicolons_SDLShortStatement_RBRACE,
    LBRACE_OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement_RBRACE,
    LBRACE_OptSemicolons_SDLStatements_Semicolons_RBRACE,
}

#[derive(Debug, Clone)]
pub enum SDLDocument {
    OptSemicolons,
    OptSemicolons_SDLStatements,
    OptSemicolons_SDLStatements_Semicolons,
    OptSemicolons_SDLShortStatement,
    OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement,
}

#[derive(Debug, Clone)]
pub enum SDLShortStatement {
    AliasDeclarationShort,
    AnnotationDeclarationShort,
    ConstraintDeclarationShort,
    ExtensionRequirementDeclaration,
    FunctionDeclarationShort,
    FutureRequirementDeclaration,
    GlobalDeclarationShort,
    IndexDeclarationShort,
    LinkDeclarationShort,
    ObjectTypeDeclarationShort,
    PropertyDeclarationShort,
    ScalarTypeDeclarationShort,
}

#[derive(Debug, Clone)]
pub enum SDLStatement {
    SDLBlockStatement,
    SDLShortStatement_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum SDLStatements {
    SDLStatement,
    SDLStatements_OptSemicolons_SDLStatement,
}

#[derive(Debug, Clone)]
pub enum ScalarTypeDeclaration {
    ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
    SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum ScalarTypeDeclarationShort {
    ABSTRACT_SCALAR_TYPE_NodeName_OptExtending,
    SCALAR_TYPE_NodeName_OptExtending,
}

#[derive(Debug, Clone)]
pub enum SchemaItem {
    SchemaObjectClass_NodeName,
}

#[derive(Debug, Clone)]
pub enum SchemaObjectClass {
    ALIAS,
    ANNOTATION,
    CAST,
    CONSTRAINT,
    FUNCTION,
    LINK,
    MODULE,
    OPERATOR,
    PROPERTY,
    SCALAR_TYPE,
    TYPE,
}

#[derive(Debug, Clone)]
pub enum SelectLimit {
    LimitClause,
    OffsetClause,
    OffsetClause_LimitClause,
}

#[derive(Debug, Clone)]
pub enum Semicolons {
    SEMICOLON,
    Semicolons_SEMICOLON,
}

#[derive(Debug, Clone)]
pub enum SessionStmt {
    ResetStmt,
    SetStmt,
}

#[derive(Debug, Clone)]
pub enum Set {
    LBRACE_OptExprList_RBRACE,
}

#[derive(Debug, Clone)]
pub enum SetAnnotation {
    ANNOTATION_NodeName_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum SetCardinalityStmt {
    RESET_CARDINALITY_OptAlterUsingClause,
    SET_MULTI,
    SET_SINGLE_OptAlterUsingClause,
}

#[derive(Debug, Clone)]
pub enum SetDelegatedStmt {
    RESET_DELEGATED,
    SET_DELEGATED,
    SET_NOT_DELEGATED,
}

#[derive(Debug, Clone)]
pub enum SetField {
    Identifier_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum SetFieldStmt {
    SET_Identifier_ASSIGN_Expr,
}

#[derive(Debug, Clone)]
pub enum SetGlobalTypeStmt {
    RESET_TYPE,
    SETTYPE_FullTypeExpr_OptAlterUsingClause,
    SETTYPE_FullTypeExpr_RESET_TO_DEFAULT,
}

#[derive(Debug, Clone)]
pub enum SetPointerTypeStmt {
    RESET_TYPE,
    SETTYPE_FullTypeExpr_OptAlterUsingClause,
}

#[derive(Debug, Clone)]
pub enum SetRequiredInCreateStmt {
    SET_REQUIRED_OptAlterUsingClause,
}

#[derive(Debug, Clone)]
pub enum SetRequiredStmt {
    DROP_REQUIRED,
    RESET_OPTIONALITY,
    SET_OPTIONAL,
    SET_REQUIRED_OptAlterUsingClause,
}

#[derive(Debug, Clone)]
pub enum SetStmt {
    SET_ALIAS_Identifier_AS_MODULE_ModuleName,
    SET_MODULE_ModuleName,
}

#[derive(Debug, Clone)]
pub enum Shape {
    LBRACE_RBRACE,
    LBRACE_ShapeElementList_RBRACE,
}

#[derive(Debug, Clone)]
pub enum ShapeElement {
    ComputableShapePointer,
    ShapePointer_OptAnySubShape_OptFilterClause_OptSortClause_OptSelectLimit,
}

#[derive(Debug, Clone)]
pub enum ShapeElementList {
    ShapeElementListInner,
    ShapeElementListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum ShapeElementListInner {
    ShapeElement,
    ShapeElementListInner_COMMA_ShapeElement,
}

#[derive(Debug, Clone)]
pub enum ShapePath {
    AT_PathNodeName,
    PathStepName_OptTypeIntersection,
    Splat,
    TypeIntersection_DOT_PathStepName_OptTypeIntersection,
}

#[derive(Debug, Clone)]
pub enum ShapePointer {
    ShapePath,
}

#[derive(Debug, Clone)]
pub enum ShortExtending {
    EXTENDING_ShortNodeNameList,
}

#[derive(Debug, Clone)]
pub enum ShortNodeName {
    Identifier,
}

#[derive(Debug, Clone)]
pub enum ShortNodeNameList {
    ShortNodeName,
    ShortNodeNameList_COMMA_ShortNodeName,
}

#[derive(Debug, Clone)]
pub enum SimpleDelete {
    DELETE_Expr_OptFilterClause_OptSortClause_OptSelectLimit,
}

#[derive(Debug, Clone)]
pub enum SimpleFor {
    FOR_OptionalOptional_Identifier_IN_AtomicExpr_UNION_Expr,
    FOR_OptionalOptional_Identifier_IN_AtomicExpr_ExprStmt,
}

#[derive(Debug, Clone)]
pub enum SimpleGroup {
    GROUP_OptionallyAliasedExpr_OptUsingClause_ByClause,
}

#[derive(Debug, Clone)]
pub enum SimpleInsert {
    INSERT_Expr_OptUnlessConflictClause,
}

#[derive(Debug, Clone)]
pub enum SimpleSelect {
    SELECT_OptionallyAliasedExpr_OptFilterClause_OptSortClause_OptSelectLimit,
}

#[derive(Debug, Clone)]
pub enum SimpleShapePath {
    AT_PathNodeName,
    PathStepName,
}

#[derive(Debug, Clone)]
pub enum SimpleShapePointer {
    SimpleShapePath,
}

#[derive(Debug, Clone)]
pub enum SimpleTypeName {
    ANYOBJECT,
    ANYTUPLE,
    ANYTYPE,
    PtrNodeName,
}

#[derive(Debug, Clone)]
pub enum SimpleTypeNameList {
    SimpleTypeName,
    SimpleTypeNameList_COMMA_SimpleTypeName,
}

#[derive(Debug, Clone)]
pub enum SimpleUpdate {
    UPDATE_Expr_OptFilterClause_SET_Shape,
}

#[derive(Debug, Clone)]
pub enum SingleStatement {
    ConfigStmt,
    DDLStmt,
    IfThenElseExpr,
    SessionStmt,
    Stmt,
}

#[derive(Debug, Clone)]
pub enum SortClause {
    ORDERBY_OrderbyList,
}

#[derive(Debug, Clone)]
pub enum Splat {
    DOUBLESTAR,
    ParenTypeExpr_DOT_DOUBLESTAR,
    ParenTypeExpr_DOT_STAR,
    ParenTypeExpr_TypeIntersection_DOT_DOUBLESTAR,
    ParenTypeExpr_TypeIntersection_DOT_STAR,
    PathStepName_DOT_DOUBLESTAR,
    PathStepName_DOT_STAR,
    PathStepName_TypeIntersection_DOT_DOUBLESTAR,
    PathStepName_TypeIntersection_DOT_STAR,
    PtrQualifiedNodeName_DOT_DOUBLESTAR,
    PtrQualifiedNodeName_DOT_STAR,
    PtrQualifiedNodeName_TypeIntersection_DOT_DOUBLESTAR,
    PtrQualifiedNodeName_TypeIntersection_DOT_STAR,
    STAR,
    TypeIntersection_DOT_DOUBLESTAR,
    TypeIntersection_DOT_STAR,
}

#[derive(Debug, Clone)]
pub enum StartMigrationStmt {
    START_MIGRATION_TO_SDLCommandBlock,
    START_MIGRATION_REWRITE,
    START_MIGRATION_TO_COMMITTED_SCHEMA,
}

#[derive(Debug, Clone)]
pub enum StatementBlock {
    SingleStatement,
    StatementBlock_Semicolons_SingleStatement,
}

#[derive(Debug, Clone)]
pub enum Stmt {
    AdministerStmt,
    AnalyzeStmt,
    DescribeStmt,
    ExprStmt,
    TransactionStmt,
}

#[derive(Debug, Clone)]
pub enum Subtype {
    BaseNumberConstant,
    BaseStringConstant,
    FullTypeExpr,
    Identifier_COLON_FullTypeExpr,
}

#[derive(Debug, Clone)]
pub enum SubtypeList {
    SubtypeListInner,
    SubtypeListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum SubtypeListInner {
    Subtype,
    SubtypeListInner_COMMA_Subtype,
}

#[derive(Debug, Clone)]
pub enum TransactionMode {
    DEFERRABLE,
    ISOLATION_SERIALIZABLE,
    NOT_DEFERRABLE,
    READ_ONLY,
    READ_WRITE,
}

#[derive(Debug, Clone)]
pub enum TransactionModeList {
    TransactionMode,
    TransactionModeList_COMMA_TransactionMode,
}

#[derive(Debug, Clone)]
pub enum TransactionStmt {
    COMMIT,
    DECLARE_SAVEPOINT_Identifier,
    RELEASE_SAVEPOINT_Identifier,
    ROLLBACK,
    ROLLBACK_TO_SAVEPOINT_Identifier,
    START_TRANSACTION_OptTransactionModeList,
}

#[derive(Debug, Clone)]
pub enum TriggerDeclarationBlock {
    TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_CreateTriggerSDLCommandsBlock,
}

#[derive(Debug, Clone)]
pub enum TriggerDeclarationShort {
    TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum TriggerKind {
    DELETE,
    INSERT,
    UPDATE,
}

#[derive(Debug, Clone)]
pub enum TriggerKindList {
    TriggerKind,
    TriggerKindList_COMMA_TriggerKind,
}

#[derive(Debug, Clone)]
pub enum TriggerScope {
    ALL,
    EACH,
}

#[derive(Debug, Clone)]
pub enum TriggerTiming {
    AFTER,
    AFTER_COMMIT_OF,
}

#[derive(Debug, Clone)]
pub enum Tuple {
    LPAREN_Expr_COMMA_OptExprList_RPAREN,
    LPAREN_RPAREN,
}

#[derive(Debug, Clone)]
pub enum TypeExpr {
    NontrivialTypeExpr,
    SimpleTypeName,
}

#[derive(Debug, Clone)]
pub enum TypeIntersection {
    LBRACKET_IS_FullTypeExpr_RBRACKET,
}

#[derive(Debug, Clone)]
pub enum TypeName {
    CollectionTypeName,
    SimpleTypeName,
}

#[derive(Debug, Clone)]
pub enum TypeNameList {
    TypeName,
    TypeNameList_COMMA_TypeName,
}

#[derive(Debug, Clone)]
pub enum UnlessConflictCause {
    UNLESS_CONFLICT_UnlessConflictSpecifier,
}

#[derive(Debug, Clone)]
pub enum UnlessConflictSpecifier {
    ON_Expr,
    ON_Expr_ELSE_Expr,
    epsilon,
}

#[derive(Debug, Clone)]
pub enum UnqualifiedPointerName {
    PointerName,
}

#[derive(Debug, Clone)]
pub enum UnreservedKeyword {
    ABORT,
    ABSTRACT,
    ACCESS,
    AFTER,
    ALIAS,
    ALL,
    ALLOW,
    ANNOTATION,
    APPLIED,
    AS,
    ASC,
    ASSIGNMENT,
    BEFORE,
    BLOBAL,
    BRANCH,
    CARDINALITY,
    CAST,
    COMMITTED,
    CONFIG,
    CONFLICT,
    CONSTRAINT,
    CUBE,
    CURRENT,
    DATA,
    DATABASE,
    DDL,
    DECLARE,
    DEFAULT,
    DEFERRABLE,
    DEFERRED,
    DELEGATED,
    DENY,
    DESC,
    EACH,
    EMPTY,
    EXPRESSION,
    EXTENSION,
    FINAL,
    FIRST,
    FORCE,
    FROM,
    FUNCTION,
    FUTURE,
    IMPLICIT,
    INDEX,
    INFIX,
    INHERITABLE,
    INSTANCE,
    INTO,
    ISOLATION,
    JSON,
    LAST,
    LINK,
    MIGRATION,
    MULTI,
    NAMED,
    OBJECT,
    OF,
    ONLY,
    ONTO,
    OPERATOR,
    OPTIONALITY,
    ORDER,
    ORPHAN,
    OVERLOADED,
    OWNED,
    PACKAGE,
    POLICY,
    POPULATE,
    POSTFIX,
    PREFIX,
    PROPERTY,
    PROPOSED,
    PSEUDO,
    READ,
    REJECT,
    RELEASE,
    RENAME,
    REQUIRED,
    RESET,
    RESTRICT,
    REWRITE,
    ROLE,
    ROLES,
    ROLLUP,
    SAVEPOINT,
    SCALAR,
    SCHEMA,
    SDL,
    SERIALIZABLE,
    SESSION,
    SOURCE,
    SUPERUSER,
    SYSTEM,
    TARGET,
    TEMPLATE,
    TERNARY,
    TEXT,
    THEN,
    TO,
    TRANSACTION,
    TRIGGER,
    TYPE,
    UNLESS,
    USING,
    VERBOSE,
    VERSION,
    VIEW,
    WRITE,
}

#[derive(Debug, Clone)]
pub enum Using {
    USING_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum UsingClause {
    USING_AliasedExprList,
}

#[derive(Debug, Clone)]
pub enum UsingStmt {
    RESET_EXPRESSION,
    USING_ParenExpr,
}

#[derive(Debug, Clone)]
pub enum WithBlock {
    WITH_WithDeclList,
}

#[derive(Debug, Clone)]
pub enum WithDDLStmt {
    InnerDDLStmt,
}

#[derive(Debug, Clone)]
pub enum WithDecl {
    AliasDecl,
}

#[derive(Debug, Clone)]
pub enum WithDeclList {
    WithDeclListInner,
    WithDeclListInner_COMMA,
}

#[derive(Debug, Clone)]
pub enum WithDeclListInner {
    WithDecl,
    WithDeclListInner_COMMA_WithDecl,
}

pub fn reduction_from_id(id: usize) -> Reduction {
    match id {
        0 => Reduction::AbortMigrationStmt(AbortMigrationStmt::ABORT_MIGRATION),
        1 => Reduction::AbortMigrationStmt(AbortMigrationStmt::ABORT_MIGRATION_REWRITE),
        2 => Reduction::AccessKind(AccessKind::ALL),
        3 => Reduction::AccessKind(AccessKind::DELETE),
        4 => Reduction::AccessKind(AccessKind::INSERT),
        5 => Reduction::AccessKind(AccessKind::SELECT),
        6 => Reduction::AccessKind(AccessKind::UPDATE),
        7 => Reduction::AccessKind(AccessKind::UPDATE_READ),
        8 => Reduction::AccessKind(AccessKind::UPDATE_WRITE),
        9 => Reduction::AccessKindList(AccessKindList::AccessKind),
        10 => Reduction::AccessKindList(AccessKindList::AccessKindList_COMMA_AccessKind),
        11 => Reduction::AccessPermStmt(AccessPermStmt::AccessPolicyAction_AccessKindList),
        12 => Reduction::AccessPolicyAction(AccessPolicyAction::ALLOW),
        13 => Reduction::AccessPolicyAction(AccessPolicyAction::DENY),
        14 => Reduction::AccessPolicyDeclarationBlock(AccessPolicyDeclarationBlock::ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_CreateAccessPolicySDLCommandsBlock),
        15 => Reduction::AccessPolicyDeclarationShort(AccessPolicyDeclarationShort::ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock),
        16 => Reduction::AccessUsingStmt(AccessUsingStmt::RESET_EXPRESSION),
        17 => Reduction::AccessUsingStmt(AccessUsingStmt::USING_ParenExpr),
        18 => Reduction::AccessWhenStmt(AccessWhenStmt::RESET_WHEN),
        19 => Reduction::AccessWhenStmt(AccessWhenStmt::WHEN_ParenExpr),
        20 => Reduction::AdministerStmt(AdministerStmt::ADMINISTER_FuncExpr),
        21 => Reduction::AliasDecl(AliasDecl::AliasedExpr),
        22 => Reduction::AliasDecl(AliasDecl::Identifier_AS_MODULE_ModuleName),
        23 => Reduction::AliasDecl(AliasDecl::MODULE_ModuleName),
        24 => Reduction::AliasDeclaration(AliasDeclaration::ALIAS_NodeName_CreateAliasSDLCommandsBlock),
        25 => Reduction::AliasDeclarationShort(AliasDeclarationShort::ALIAS_NodeName_CreateAliasSingleSDLCommandBlock),
        26 => Reduction::AliasDeclarationShort(AliasDeclarationShort::ALIAS_NodeName_ASSIGN_Expr),
        27 => Reduction::AliasedExpr(AliasedExpr::Identifier_ASSIGN_Expr),
        28 => Reduction::AliasedExprList(AliasedExprList::AliasedExprListInner),
        29 => Reduction::AliasedExprList(AliasedExprList::AliasedExprListInner_COMMA),
        30 => Reduction::AliasedExprListInner(AliasedExprListInner::AliasedExpr),
        31 => Reduction::AliasedExprListInner(AliasedExprListInner::AliasedExprListInner_COMMA_AliasedExpr),
        32 => Reduction::AlterAbstract(AlterAbstract::DROP_ABSTRACT),
        33 => Reduction::AlterAbstract(AlterAbstract::RESET_ABSTRACT),
        34 => Reduction::AlterAbstract(AlterAbstract::SET_ABSTRACT),
        35 => Reduction::AlterAbstract(AlterAbstract::SET_NOT_ABSTRACT),
        36 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::AccessPermStmt),
        37 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::AccessUsingStmt),
        38 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::AccessWhenStmt),
        39 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::AlterAnnotationValueStmt),
        40 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::CreateAnnotationValueStmt),
        41 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::DropAnnotationValueStmt),
        42 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::RenameStmt),
        43 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::ResetFieldStmt),
        44 => Reduction::AlterAccessPolicyCommand(AlterAccessPolicyCommand::SetFieldStmt),
        45 => Reduction::AlterAccessPolicyCommandsBlock(AlterAccessPolicyCommandsBlock::AlterAccessPolicyCommand),
        46 => Reduction::AlterAccessPolicyCommandsBlock(AlterAccessPolicyCommandsBlock::LBRACE_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE),
        47 => Reduction::AlterAccessPolicyCommandsBlock(AlterAccessPolicyCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        48 => Reduction::AlterAccessPolicyCommandsBlock(AlterAccessPolicyCommandsBlock::LBRACE_Semicolons_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE),
        49 => Reduction::AlterAccessPolicyCommandsList(AlterAccessPolicyCommandsList::AlterAccessPolicyCommand),
        50 => Reduction::AlterAccessPolicyCommandsList(AlterAccessPolicyCommandsList::AlterAccessPolicyCommandsList_Semicolons_AlterAccessPolicyCommand),
        51 => Reduction::AlterAccessPolicyStmt(AlterAccessPolicyStmt::ALTER_ACCESS_POLICY_UnqualifiedPointerName_AlterAccessPolicyCommandsBlock),
        52 => Reduction::AlterAliasCommand(AlterAliasCommand::AlterAnnotationValueStmt),
        53 => Reduction::AlterAliasCommand(AlterAliasCommand::CreateAnnotationValueStmt),
        54 => Reduction::AlterAliasCommand(AlterAliasCommand::DropAnnotationValueStmt),
        55 => Reduction::AlterAliasCommand(AlterAliasCommand::RenameStmt),
        56 => Reduction::AlterAliasCommand(AlterAliasCommand::ResetFieldStmt),
        57 => Reduction::AlterAliasCommand(AlterAliasCommand::SetFieldStmt),
        58 => Reduction::AlterAliasCommand(AlterAliasCommand::UsingStmt),
        59 => Reduction::AlterAliasCommandsBlock(AlterAliasCommandsBlock::AlterAliasCommand),
        60 => Reduction::AlterAliasCommandsBlock(AlterAliasCommandsBlock::LBRACE_AlterAliasCommandsList_OptSemicolons_RBRACE),
        61 => Reduction::AlterAliasCommandsBlock(AlterAliasCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        62 => Reduction::AlterAliasCommandsBlock(AlterAliasCommandsBlock::LBRACE_Semicolons_AlterAliasCommandsList_OptSemicolons_RBRACE),
        63 => Reduction::AlterAliasCommandsList(AlterAliasCommandsList::AlterAliasCommand),
        64 => Reduction::AlterAliasCommandsList(AlterAliasCommandsList::AlterAliasCommandsList_Semicolons_AlterAliasCommand),
        65 => Reduction::AlterAliasStmt(AlterAliasStmt::ALTER_ALIAS_NodeName_AlterAliasCommandsBlock),
        66 => Reduction::AlterAnnotationCommand(AlterAnnotationCommand::AlterAnnotationValueStmt),
        67 => Reduction::AlterAnnotationCommand(AlterAnnotationCommand::CreateAnnotationValueStmt),
        68 => Reduction::AlterAnnotationCommand(AlterAnnotationCommand::DropAnnotationValueStmt),
        69 => Reduction::AlterAnnotationCommand(AlterAnnotationCommand::RenameStmt),
        70 => Reduction::AlterAnnotationCommandsBlock(AlterAnnotationCommandsBlock::AlterAnnotationCommand),
        71 => Reduction::AlterAnnotationCommandsBlock(AlterAnnotationCommandsBlock::LBRACE_AlterAnnotationCommandsList_OptSemicolons_RBRACE),
        72 => Reduction::AlterAnnotationCommandsBlock(AlterAnnotationCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        73 => Reduction::AlterAnnotationCommandsBlock(AlterAnnotationCommandsBlock::LBRACE_Semicolons_AlterAnnotationCommandsList_OptSemicolons_RBRACE),
        74 => Reduction::AlterAnnotationCommandsList(AlterAnnotationCommandsList::AlterAnnotationCommand),
        75 => Reduction::AlterAnnotationCommandsList(AlterAnnotationCommandsList::AlterAnnotationCommandsList_Semicolons_AlterAnnotationCommand),
        76 => Reduction::AlterAnnotationStmt(AlterAnnotationStmt::ALTER_ABSTRACT_ANNOTATION_NodeName_AlterAnnotationCommandsBlock),
        77 => Reduction::AlterAnnotationValueStmt(AlterAnnotationValueStmt::ALTER_ANNOTATION_NodeName_ASSIGN_Expr),
        78 => Reduction::AlterAnnotationValueStmt(AlterAnnotationValueStmt::ALTER_ANNOTATION_NodeName_DROP_OWNED),
        79 => Reduction::AlterBranchCommand(AlterBranchCommand::RenameStmt),
        80 => Reduction::AlterBranchCommandsBlock(AlterBranchCommandsBlock::AlterBranchCommand),
        81 => Reduction::AlterBranchCommandsBlock(AlterBranchCommandsBlock::LBRACE_AlterBranchCommandsList_OptSemicolons_RBRACE),
        82 => Reduction::AlterBranchCommandsBlock(AlterBranchCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        83 => Reduction::AlterBranchCommandsBlock(AlterBranchCommandsBlock::LBRACE_Semicolons_AlterBranchCommandsList_OptSemicolons_RBRACE),
        84 => Reduction::AlterBranchCommandsList(AlterBranchCommandsList::AlterBranchCommand),
        85 => Reduction::AlterBranchCommandsList(AlterBranchCommandsList::AlterBranchCommandsList_Semicolons_AlterBranchCommand),
        86 => Reduction::AlterBranchStmt(AlterBranchStmt::ALTER_BRANCH_DatabaseName_BranchOptions_AlterBranchCommandsBlock),
        87 => Reduction::AlterCastCommand(AlterCastCommand::AlterAnnotationValueStmt),
        88 => Reduction::AlterCastCommand(AlterCastCommand::CreateAnnotationValueStmt),
        89 => Reduction::AlterCastCommand(AlterCastCommand::DropAnnotationValueStmt),
        90 => Reduction::AlterCastCommand(AlterCastCommand::ResetFieldStmt),
        91 => Reduction::AlterCastCommand(AlterCastCommand::SetFieldStmt),
        92 => Reduction::AlterCastCommandsBlock(AlterCastCommandsBlock::AlterCastCommand),
        93 => Reduction::AlterCastCommandsBlock(AlterCastCommandsBlock::LBRACE_AlterCastCommandsList_OptSemicolons_RBRACE),
        94 => Reduction::AlterCastCommandsBlock(AlterCastCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        95 => Reduction::AlterCastCommandsBlock(AlterCastCommandsBlock::LBRACE_Semicolons_AlterCastCommandsList_OptSemicolons_RBRACE),
        96 => Reduction::AlterCastCommandsList(AlterCastCommandsList::AlterCastCommand),
        97 => Reduction::AlterCastCommandsList(AlterCastCommandsList::AlterCastCommandsList_Semicolons_AlterCastCommand),
        98 => Reduction::AlterCastStmt(AlterCastStmt::ALTER_CAST_FROM_TypeName_TO_TypeName_AlterCastCommandsBlock),
        99 => Reduction::AlterCommand(AlterCommand::AlterAnnotationValueStmt),
        100 => Reduction::AlterCommand(AlterCommand::CreateAnnotationValueStmt),
        101 => Reduction::AlterCommand(AlterCommand::DropAnnotationValueStmt),
        102 => Reduction::AlterCommand(AlterCommand::RenameStmt),
        103 => Reduction::AlterCommand(AlterCommand::ResetFieldStmt),
        104 => Reduction::AlterCommand(AlterCommand::SetFieldStmt),
        105 => Reduction::AlterCommand(AlterCommand::UsingStmt),
        106 => Reduction::AlterCommandsBlock(AlterCommandsBlock::AlterCommand),
        107 => Reduction::AlterCommandsBlock(AlterCommandsBlock::LBRACE_AlterCommandsList_OptSemicolons_RBRACE),
        108 => Reduction::AlterCommandsBlock(AlterCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        109 => Reduction::AlterCommandsBlock(AlterCommandsBlock::LBRACE_Semicolons_AlterCommandsList_OptSemicolons_RBRACE),
        110 => Reduction::AlterCommandsList(AlterCommandsList::AlterCommand),
        111 => Reduction::AlterCommandsList(AlterCommandsList::AlterCommandsList_Semicolons_AlterCommand),
        112 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::AlterAbstract),
        113 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::AlterAnnotationValueStmt),
        114 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::AlterOwnedStmt),
        115 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::CreateAnnotationValueStmt),
        116 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::DropAnnotationValueStmt),
        117 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::ResetFieldStmt),
        118 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::SetDelegatedStmt),
        119 => Reduction::AlterConcreteConstraintCommand(AlterConcreteConstraintCommand::SetFieldStmt),
        120 => Reduction::AlterConcreteConstraintCommandsBlock(AlterConcreteConstraintCommandsBlock::AlterConcreteConstraintCommand),
        121 => Reduction::AlterConcreteConstraintCommandsBlock(AlterConcreteConstraintCommandsBlock::LBRACE_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE),
        122 => Reduction::AlterConcreteConstraintCommandsBlock(AlterConcreteConstraintCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        123 => Reduction::AlterConcreteConstraintCommandsBlock(AlterConcreteConstraintCommandsBlock::LBRACE_Semicolons_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE),
        124 => Reduction::AlterConcreteConstraintCommandsList(AlterConcreteConstraintCommandsList::AlterConcreteConstraintCommand),
        125 => Reduction::AlterConcreteConstraintCommandsList(AlterConcreteConstraintCommandsList::AlterConcreteConstraintCommandsList_Semicolons_AlterConcreteConstraintCommand),
        126 => Reduction::AlterConcreteConstraintStmt(AlterConcreteConstraintStmt::ALTER_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_AlterConcreteConstraintCommandsBlock),
        127 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::AlterAnnotationValueStmt),
        128 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::AlterDeferredStmt),
        129 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::AlterOwnedStmt),
        130 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::CreateAnnotationValueStmt),
        131 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::DropAnnotationValueStmt),
        132 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::ResetFieldStmt),
        133 => Reduction::AlterConcreteIndexCommand(AlterConcreteIndexCommand::SetFieldStmt),
        134 => Reduction::AlterConcreteIndexCommandsBlock(AlterConcreteIndexCommandsBlock::AlterConcreteIndexCommand),
        135 => Reduction::AlterConcreteIndexCommandsBlock(AlterConcreteIndexCommandsBlock::LBRACE_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE),
        136 => Reduction::AlterConcreteIndexCommandsBlock(AlterConcreteIndexCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        137 => Reduction::AlterConcreteIndexCommandsBlock(AlterConcreteIndexCommandsBlock::LBRACE_Semicolons_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE),
        138 => Reduction::AlterConcreteIndexCommandsList(AlterConcreteIndexCommandsList::AlterConcreteIndexCommand),
        139 => Reduction::AlterConcreteIndexCommandsList(AlterConcreteIndexCommandsList::AlterConcreteIndexCommandsList_Semicolons_AlterConcreteIndexCommand),
        140 => Reduction::AlterConcreteIndexStmt(AlterConcreteIndexStmt::ALTER_INDEX_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock),
        141 => Reduction::AlterConcreteIndexStmt(AlterConcreteIndexStmt::ALTER_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock),
        142 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterAnnotationValueStmt),
        143 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterConcreteConstraintStmt),
        144 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterConcreteIndexStmt),
        145 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterConcretePropertyStmt),
        146 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterOwnedStmt),
        147 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterRewriteStmt),
        148 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::AlterSimpleExtending),
        149 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::CreateAnnotationValueStmt),
        150 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::CreateConcreteConstraintStmt),
        151 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::CreateConcreteIndexStmt),
        152 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::CreateConcretePropertyStmt),
        153 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::CreateRewriteStmt),
        154 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::DropAnnotationValueStmt),
        155 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::DropConcreteConstraintStmt),
        156 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::DropConcreteIndexStmt),
        157 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::DropConcretePropertyStmt),
        158 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::DropRewriteStmt),
        159 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::OnSourceDeleteResetStmt),
        160 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::OnSourceDeleteStmt),
        161 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::OnTargetDeleteResetStmt),
        162 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::OnTargetDeleteStmt),
        163 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::RenameStmt),
        164 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::ResetFieldStmt),
        165 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::SetCardinalityStmt),
        166 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::SetFieldStmt),
        167 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::SetPointerTypeStmt),
        168 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::SetRequiredStmt),
        169 => Reduction::AlterConcreteLinkCommand(AlterConcreteLinkCommand::UsingStmt),
        170 => Reduction::AlterConcreteLinkCommandsBlock(AlterConcreteLinkCommandsBlock::AlterConcreteLinkCommand),
        171 => Reduction::AlterConcreteLinkCommandsBlock(AlterConcreteLinkCommandsBlock::LBRACE_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE),
        172 => Reduction::AlterConcreteLinkCommandsBlock(AlterConcreteLinkCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        173 => Reduction::AlterConcreteLinkCommandsBlock(AlterConcreteLinkCommandsBlock::LBRACE_Semicolons_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE),
        174 => Reduction::AlterConcreteLinkCommandsList(AlterConcreteLinkCommandsList::AlterConcreteLinkCommand),
        175 => Reduction::AlterConcreteLinkCommandsList(AlterConcreteLinkCommandsList::AlterConcreteLinkCommandsList_Semicolons_AlterConcreteLinkCommand),
        176 => Reduction::AlterConcreteLinkStmt(AlterConcreteLinkStmt::ALTER_LINK_UnqualifiedPointerName_AlterConcreteLinkCommandsBlock),
        177 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::AlterAnnotationValueStmt),
        178 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::AlterConcreteConstraintStmt),
        179 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::AlterOwnedStmt),
        180 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::AlterRewriteStmt),
        181 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::AlterSimpleExtending),
        182 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::CreateAnnotationValueStmt),
        183 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::CreateConcreteConstraintStmt),
        184 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::CreateRewriteStmt),
        185 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::DropAnnotationValueStmt),
        186 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::DropConcreteConstraintStmt),
        187 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::DropRewriteStmt),
        188 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::RenameStmt),
        189 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::ResetFieldStmt),
        190 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::SetCardinalityStmt),
        191 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::SetFieldStmt),
        192 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::SetPointerTypeStmt),
        193 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::SetRequiredStmt),
        194 => Reduction::AlterConcretePropertyCommand(AlterConcretePropertyCommand::UsingStmt),
        195 => Reduction::AlterConcretePropertyCommandsBlock(AlterConcretePropertyCommandsBlock::AlterConcretePropertyCommand),
        196 => Reduction::AlterConcretePropertyCommandsBlock(AlterConcretePropertyCommandsBlock::LBRACE_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE),
        197 => Reduction::AlterConcretePropertyCommandsBlock(AlterConcretePropertyCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        198 => Reduction::AlterConcretePropertyCommandsBlock(AlterConcretePropertyCommandsBlock::LBRACE_Semicolons_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE),
        199 => Reduction::AlterConcretePropertyCommandsList(AlterConcretePropertyCommandsList::AlterConcretePropertyCommand),
        200 => Reduction::AlterConcretePropertyCommandsList(AlterConcretePropertyCommandsList::AlterConcretePropertyCommandsList_Semicolons_AlterConcretePropertyCommand),
        201 => Reduction::AlterConcretePropertyStmt(AlterConcretePropertyStmt::ALTER_PROPERTY_UnqualifiedPointerName_AlterConcretePropertyCommandsBlock),
        202 => Reduction::AlterConstraintStmt(AlterConstraintStmt::ALTER_ABSTRACT_CONSTRAINT_NodeName_AlterCommandsBlock),
        203 => Reduction::AlterCurrentMigrationStmt(AlterCurrentMigrationStmt::ALTER_CURRENT_MIGRATION_REJECT_PROPOSED),
        204 => Reduction::AlterDatabaseCommand(AlterDatabaseCommand::RenameStmt),
        205 => Reduction::AlterDatabaseCommandsBlock(AlterDatabaseCommandsBlock::AlterDatabaseCommand),
        206 => Reduction::AlterDatabaseCommandsBlock(AlterDatabaseCommandsBlock::LBRACE_AlterDatabaseCommandsList_OptSemicolons_RBRACE),
        207 => Reduction::AlterDatabaseCommandsBlock(AlterDatabaseCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        208 => Reduction::AlterDatabaseCommandsBlock(AlterDatabaseCommandsBlock::LBRACE_Semicolons_AlterDatabaseCommandsList_OptSemicolons_RBRACE),
        209 => Reduction::AlterDatabaseCommandsList(AlterDatabaseCommandsList::AlterDatabaseCommand),
        210 => Reduction::AlterDatabaseCommandsList(AlterDatabaseCommandsList::AlterDatabaseCommandsList_Semicolons_AlterDatabaseCommand),
        211 => Reduction::AlterDatabaseStmt(AlterDatabaseStmt::ALTER_DATABASE_DatabaseName_AlterDatabaseCommandsBlock),
        212 => Reduction::AlterDeferredStmt(AlterDeferredStmt::DROP_DEFERRED),
        213 => Reduction::AlterDeferredStmt(AlterDeferredStmt::SET_DEFERRED),
        214 => Reduction::AlterExtending(AlterExtending::AlterAbstract),
        215 => Reduction::AlterExtending(AlterExtending::DROP_EXTENDING_TypeNameList),
        216 => Reduction::AlterExtending(AlterExtending::EXTENDING_TypeNameList_OptPosition),
        217 => Reduction::AlterFunctionCommand(AlterFunctionCommand::AlterAnnotationValueStmt),
        218 => Reduction::AlterFunctionCommand(AlterFunctionCommand::CreateAnnotationValueStmt),
        219 => Reduction::AlterFunctionCommand(AlterFunctionCommand::DropAnnotationValueStmt),
        220 => Reduction::AlterFunctionCommand(AlterFunctionCommand::FromFunction),
        221 => Reduction::AlterFunctionCommand(AlterFunctionCommand::RenameStmt),
        222 => Reduction::AlterFunctionCommand(AlterFunctionCommand::ResetFieldStmt),
        223 => Reduction::AlterFunctionCommand(AlterFunctionCommand::SetFieldStmt),
        224 => Reduction::AlterFunctionCommandsBlock(AlterFunctionCommandsBlock::AlterFunctionCommand),
        225 => Reduction::AlterFunctionCommandsBlock(AlterFunctionCommandsBlock::LBRACE_AlterFunctionCommandsList_OptSemicolons_RBRACE),
        226 => Reduction::AlterFunctionCommandsBlock(AlterFunctionCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        227 => Reduction::AlterFunctionCommandsBlock(AlterFunctionCommandsBlock::LBRACE_Semicolons_AlterFunctionCommandsList_OptSemicolons_RBRACE),
        228 => Reduction::AlterFunctionCommandsList(AlterFunctionCommandsList::AlterFunctionCommand),
        229 => Reduction::AlterFunctionCommandsList(AlterFunctionCommandsList::AlterFunctionCommandsList_Semicolons_AlterFunctionCommand),
        230 => Reduction::AlterFunctionStmt(AlterFunctionStmt::ALTER_FUNCTION_NodeName_CreateFunctionArgs_AlterFunctionCommandsBlock),
        231 => Reduction::AlterGlobalCommand(AlterGlobalCommand::AlterAnnotationValueStmt),
        232 => Reduction::AlterGlobalCommand(AlterGlobalCommand::CreateAnnotationValueStmt),
        233 => Reduction::AlterGlobalCommand(AlterGlobalCommand::DropAnnotationValueStmt),
        234 => Reduction::AlterGlobalCommand(AlterGlobalCommand::RenameStmt),
        235 => Reduction::AlterGlobalCommand(AlterGlobalCommand::ResetFieldStmt),
        236 => Reduction::AlterGlobalCommand(AlterGlobalCommand::SetCardinalityStmt),
        237 => Reduction::AlterGlobalCommand(AlterGlobalCommand::SetFieldStmt),
        238 => Reduction::AlterGlobalCommand(AlterGlobalCommand::SetGlobalTypeStmt),
        239 => Reduction::AlterGlobalCommand(AlterGlobalCommand::SetRequiredStmt),
        240 => Reduction::AlterGlobalCommand(AlterGlobalCommand::UsingStmt),
        241 => Reduction::AlterGlobalCommandsBlock(AlterGlobalCommandsBlock::AlterGlobalCommand),
        242 => Reduction::AlterGlobalCommandsBlock(AlterGlobalCommandsBlock::LBRACE_AlterGlobalCommandsList_OptSemicolons_RBRACE),
        243 => Reduction::AlterGlobalCommandsBlock(AlterGlobalCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        244 => Reduction::AlterGlobalCommandsBlock(AlterGlobalCommandsBlock::LBRACE_Semicolons_AlterGlobalCommandsList_OptSemicolons_RBRACE),
        245 => Reduction::AlterGlobalCommandsList(AlterGlobalCommandsList::AlterGlobalCommand),
        246 => Reduction::AlterGlobalCommandsList(AlterGlobalCommandsList::AlterGlobalCommandsList_Semicolons_AlterGlobalCommand),
        247 => Reduction::AlterGlobalStmt(AlterGlobalStmt::ALTER_GLOBAL_NodeName_AlterGlobalCommandsBlock),
        248 => Reduction::AlterIndexCommand(AlterIndexCommand::AlterAnnotationValueStmt),
        249 => Reduction::AlterIndexCommand(AlterIndexCommand::CreateAnnotationValueStmt),
        250 => Reduction::AlterIndexCommand(AlterIndexCommand::DropAnnotationValueStmt),
        251 => Reduction::AlterIndexCommand(AlterIndexCommand::RenameStmt),
        252 => Reduction::AlterIndexCommand(AlterIndexCommand::ResetFieldStmt),
        253 => Reduction::AlterIndexCommand(AlterIndexCommand::SetFieldStmt),
        254 => Reduction::AlterIndexCommand(AlterIndexCommand::UsingStmt),
        255 => Reduction::AlterIndexCommandsBlock(AlterIndexCommandsBlock::AlterIndexCommand),
        256 => Reduction::AlterIndexCommandsBlock(AlterIndexCommandsBlock::LBRACE_AlterIndexCommandsList_OptSemicolons_RBRACE),
        257 => Reduction::AlterIndexCommandsBlock(AlterIndexCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        258 => Reduction::AlterIndexCommandsBlock(AlterIndexCommandsBlock::LBRACE_Semicolons_AlterIndexCommandsList_OptSemicolons_RBRACE),
        259 => Reduction::AlterIndexCommandsList(AlterIndexCommandsList::AlterIndexCommand),
        260 => Reduction::AlterIndexCommandsList(AlterIndexCommandsList::AlterIndexCommandsList_Semicolons_AlterIndexCommand),
        261 => Reduction::AlterIndexStmt(AlterIndexStmt::ALTER_ABSTRACT_INDEX_NodeName_AlterIndexCommandsBlock),
        262 => Reduction::AlterLinkCommand(AlterLinkCommand::AlterAnnotationValueStmt),
        263 => Reduction::AlterLinkCommand(AlterLinkCommand::AlterConcreteConstraintStmt),
        264 => Reduction::AlterLinkCommand(AlterLinkCommand::AlterConcreteIndexStmt),
        265 => Reduction::AlterLinkCommand(AlterLinkCommand::AlterConcretePropertyStmt),
        266 => Reduction::AlterLinkCommand(AlterLinkCommand::AlterRewriteStmt),
        267 => Reduction::AlterLinkCommand(AlterLinkCommand::AlterSimpleExtending),
        268 => Reduction::AlterLinkCommand(AlterLinkCommand::CreateAnnotationValueStmt),
        269 => Reduction::AlterLinkCommand(AlterLinkCommand::CreateConcreteConstraintStmt),
        270 => Reduction::AlterLinkCommand(AlterLinkCommand::CreateConcreteIndexStmt),
        271 => Reduction::AlterLinkCommand(AlterLinkCommand::CreateConcretePropertyStmt),
        272 => Reduction::AlterLinkCommand(AlterLinkCommand::CreateRewriteStmt),
        273 => Reduction::AlterLinkCommand(AlterLinkCommand::DropAnnotationValueStmt),
        274 => Reduction::AlterLinkCommand(AlterLinkCommand::DropConcreteConstraintStmt),
        275 => Reduction::AlterLinkCommand(AlterLinkCommand::DropConcreteIndexStmt),
        276 => Reduction::AlterLinkCommand(AlterLinkCommand::DropConcretePropertyStmt),
        277 => Reduction::AlterLinkCommand(AlterLinkCommand::DropRewriteStmt),
        278 => Reduction::AlterLinkCommand(AlterLinkCommand::RenameStmt),
        279 => Reduction::AlterLinkCommand(AlterLinkCommand::ResetFieldStmt),
        280 => Reduction::AlterLinkCommand(AlterLinkCommand::SetFieldStmt),
        281 => Reduction::AlterLinkCommandsBlock(AlterLinkCommandsBlock::AlterLinkCommand),
        282 => Reduction::AlterLinkCommandsBlock(AlterLinkCommandsBlock::LBRACE_AlterLinkCommandsList_OptSemicolons_RBRACE),
        283 => Reduction::AlterLinkCommandsBlock(AlterLinkCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        284 => Reduction::AlterLinkCommandsBlock(AlterLinkCommandsBlock::LBRACE_Semicolons_AlterLinkCommandsList_OptSemicolons_RBRACE),
        285 => Reduction::AlterLinkCommandsList(AlterLinkCommandsList::AlterLinkCommand),
        286 => Reduction::AlterLinkCommandsList(AlterLinkCommandsList::AlterLinkCommandsList_Semicolons_AlterLinkCommand),
        287 => Reduction::AlterLinkStmt(AlterLinkStmt::ALTER_ABSTRACT_LINK_PtrNodeName_AlterLinkCommandsBlock),
        288 => Reduction::AlterMigrationCommand(AlterMigrationCommand::ResetFieldStmt),
        289 => Reduction::AlterMigrationCommand(AlterMigrationCommand::SetFieldStmt),
        290 => Reduction::AlterMigrationCommandsBlock(AlterMigrationCommandsBlock::AlterMigrationCommand),
        291 => Reduction::AlterMigrationCommandsBlock(AlterMigrationCommandsBlock::LBRACE_AlterMigrationCommandsList_OptSemicolons_RBRACE),
        292 => Reduction::AlterMigrationCommandsBlock(AlterMigrationCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        293 => Reduction::AlterMigrationCommandsBlock(AlterMigrationCommandsBlock::LBRACE_Semicolons_AlterMigrationCommandsList_OptSemicolons_RBRACE),
        294 => Reduction::AlterMigrationCommandsList(AlterMigrationCommandsList::AlterMigrationCommand),
        295 => Reduction::AlterMigrationCommandsList(AlterMigrationCommandsList::AlterMigrationCommandsList_Semicolons_AlterMigrationCommand),
        296 => Reduction::AlterMigrationStmt(AlterMigrationStmt::ALTER_MIGRATION_NodeName_AlterMigrationCommandsBlock),
        297 => Reduction::AlterModuleStmt(AlterModuleStmt::ALTER_MODULE_ModuleName_AlterCommandsBlock),
        298 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterAccessPolicyStmt),
        299 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterAnnotationValueStmt),
        300 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterConcreteConstraintStmt),
        301 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterConcreteIndexStmt),
        302 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterConcreteLinkStmt),
        303 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterConcretePropertyStmt),
        304 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterSimpleExtending),
        305 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::AlterTriggerStmt),
        306 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateAccessPolicyStmt),
        307 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateAnnotationValueStmt),
        308 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateConcreteConstraintStmt),
        309 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateConcreteIndexStmt),
        310 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateConcreteLinkStmt),
        311 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateConcretePropertyStmt),
        312 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::CreateTriggerStmt),
        313 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropAccessPolicyStmt),
        314 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropAnnotationValueStmt),
        315 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropConcreteConstraintStmt),
        316 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropConcreteIndexStmt),
        317 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropConcreteLinkStmt),
        318 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropConcretePropertyStmt),
        319 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::DropTriggerStmt),
        320 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::RenameStmt),
        321 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::ResetFieldStmt),
        322 => Reduction::AlterObjectTypeCommand(AlterObjectTypeCommand::SetFieldStmt),
        323 => Reduction::AlterObjectTypeCommandsBlock(AlterObjectTypeCommandsBlock::AlterObjectTypeCommand),
        324 => Reduction::AlterObjectTypeCommandsBlock(AlterObjectTypeCommandsBlock::LBRACE_AlterObjectTypeCommandsList_OptSemicolons_RBRACE),
        325 => Reduction::AlterObjectTypeCommandsBlock(AlterObjectTypeCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        326 => Reduction::AlterObjectTypeCommandsBlock(AlterObjectTypeCommandsBlock::LBRACE_Semicolons_AlterObjectTypeCommandsList_OptSemicolons_RBRACE),
        327 => Reduction::AlterObjectTypeCommandsList(AlterObjectTypeCommandsList::AlterObjectTypeCommand),
        328 => Reduction::AlterObjectTypeCommandsList(AlterObjectTypeCommandsList::AlterObjectTypeCommandsList_Semicolons_AlterObjectTypeCommand),
        329 => Reduction::AlterObjectTypeStmt(AlterObjectTypeStmt::ALTER_TYPE_NodeName_AlterObjectTypeCommandsBlock),
        330 => Reduction::AlterOperatorCommand(AlterOperatorCommand::AlterAnnotationValueStmt),
        331 => Reduction::AlterOperatorCommand(AlterOperatorCommand::CreateAnnotationValueStmt),
        332 => Reduction::AlterOperatorCommand(AlterOperatorCommand::DropAnnotationValueStmt),
        333 => Reduction::AlterOperatorCommand(AlterOperatorCommand::ResetFieldStmt),
        334 => Reduction::AlterOperatorCommand(AlterOperatorCommand::SetFieldStmt),
        335 => Reduction::AlterOperatorCommandsBlock(AlterOperatorCommandsBlock::AlterOperatorCommand),
        336 => Reduction::AlterOperatorCommandsBlock(AlterOperatorCommandsBlock::LBRACE_AlterOperatorCommandsList_OptSemicolons_RBRACE),
        337 => Reduction::AlterOperatorCommandsBlock(AlterOperatorCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        338 => Reduction::AlterOperatorCommandsBlock(AlterOperatorCommandsBlock::LBRACE_Semicolons_AlterOperatorCommandsList_OptSemicolons_RBRACE),
        339 => Reduction::AlterOperatorCommandsList(AlterOperatorCommandsList::AlterOperatorCommand),
        340 => Reduction::AlterOperatorCommandsList(AlterOperatorCommandsList::AlterOperatorCommandsList_Semicolons_AlterOperatorCommand),
        341 => Reduction::AlterOperatorStmt(AlterOperatorStmt::ALTER_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_AlterOperatorCommandsBlock),
        342 => Reduction::AlterOwnedStmt(AlterOwnedStmt::DROP_OWNED),
        343 => Reduction::AlterOwnedStmt(AlterOwnedStmt::SET_OWNED),
        344 => Reduction::AlterPropertyCommand(AlterPropertyCommand::AlterAnnotationValueStmt),
        345 => Reduction::AlterPropertyCommand(AlterPropertyCommand::AlterRewriteStmt),
        346 => Reduction::AlterPropertyCommand(AlterPropertyCommand::CreateAnnotationValueStmt),
        347 => Reduction::AlterPropertyCommand(AlterPropertyCommand::CreateRewriteStmt),
        348 => Reduction::AlterPropertyCommand(AlterPropertyCommand::DropAnnotationValueStmt),
        349 => Reduction::AlterPropertyCommand(AlterPropertyCommand::DropRewriteStmt),
        350 => Reduction::AlterPropertyCommand(AlterPropertyCommand::RenameStmt),
        351 => Reduction::AlterPropertyCommand(AlterPropertyCommand::ResetFieldStmt),
        352 => Reduction::AlterPropertyCommand(AlterPropertyCommand::SetFieldStmt),
        353 => Reduction::AlterPropertyCommandsBlock(AlterPropertyCommandsBlock::AlterPropertyCommand),
        354 => Reduction::AlterPropertyCommandsBlock(AlterPropertyCommandsBlock::LBRACE_AlterPropertyCommandsList_OptSemicolons_RBRACE),
        355 => Reduction::AlterPropertyCommandsBlock(AlterPropertyCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        356 => Reduction::AlterPropertyCommandsBlock(AlterPropertyCommandsBlock::LBRACE_Semicolons_AlterPropertyCommandsList_OptSemicolons_RBRACE),
        357 => Reduction::AlterPropertyCommandsList(AlterPropertyCommandsList::AlterPropertyCommand),
        358 => Reduction::AlterPropertyCommandsList(AlterPropertyCommandsList::AlterPropertyCommandsList_Semicolons_AlterPropertyCommand),
        359 => Reduction::AlterPropertyStmt(AlterPropertyStmt::ALTER_ABSTRACT_PROPERTY_PtrNodeName_AlterPropertyCommandsBlock),
        360 => Reduction::AlterRewriteCommand(AlterRewriteCommand::AlterAnnotationValueStmt),
        361 => Reduction::AlterRewriteCommand(AlterRewriteCommand::CreateAnnotationValueStmt),
        362 => Reduction::AlterRewriteCommand(AlterRewriteCommand::DropAnnotationValueStmt),
        363 => Reduction::AlterRewriteCommand(AlterRewriteCommand::ResetFieldStmt),
        364 => Reduction::AlterRewriteCommand(AlterRewriteCommand::SetFieldStmt),
        365 => Reduction::AlterRewriteCommand(AlterRewriteCommand::UsingStmt),
        366 => Reduction::AlterRewriteCommandsBlock(AlterRewriteCommandsBlock::AlterRewriteCommand),
        367 => Reduction::AlterRewriteCommandsBlock(AlterRewriteCommandsBlock::LBRACE_AlterRewriteCommandsList_OptSemicolons_RBRACE),
        368 => Reduction::AlterRewriteCommandsBlock(AlterRewriteCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        369 => Reduction::AlterRewriteCommandsBlock(AlterRewriteCommandsBlock::LBRACE_Semicolons_AlterRewriteCommandsList_OptSemicolons_RBRACE),
        370 => Reduction::AlterRewriteCommandsList(AlterRewriteCommandsList::AlterRewriteCommand),
        371 => Reduction::AlterRewriteCommandsList(AlterRewriteCommandsList::AlterRewriteCommandsList_Semicolons_AlterRewriteCommand),
        372 => Reduction::AlterRewriteStmt(AlterRewriteStmt::ALTER_REWRITE_RewriteKindList_AlterRewriteCommandsBlock),
        373 => Reduction::AlterRoleCommand(AlterRoleCommand::AlterRoleExtending),
        374 => Reduction::AlterRoleCommand(AlterRoleCommand::RenameStmt),
        375 => Reduction::AlterRoleCommand(AlterRoleCommand::ResetFieldStmt),
        376 => Reduction::AlterRoleCommand(AlterRoleCommand::SetFieldStmt),
        377 => Reduction::AlterRoleCommandsBlock(AlterRoleCommandsBlock::AlterRoleCommand),
        378 => Reduction::AlterRoleCommandsBlock(AlterRoleCommandsBlock::LBRACE_AlterRoleCommandsList_OptSemicolons_RBRACE),
        379 => Reduction::AlterRoleCommandsBlock(AlterRoleCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        380 => Reduction::AlterRoleCommandsBlock(AlterRoleCommandsBlock::LBRACE_Semicolons_AlterRoleCommandsList_OptSemicolons_RBRACE),
        381 => Reduction::AlterRoleCommandsList(AlterRoleCommandsList::AlterRoleCommand),
        382 => Reduction::AlterRoleCommandsList(AlterRoleCommandsList::AlterRoleCommandsList_Semicolons_AlterRoleCommand),
        383 => Reduction::AlterRoleExtending(AlterRoleExtending::DROP_EXTENDING_ShortNodeNameList),
        384 => Reduction::AlterRoleExtending(AlterRoleExtending::EXTENDING_ShortNodeNameList_OptPosition),
        385 => Reduction::AlterRoleStmt(AlterRoleStmt::ALTER_ROLE_ShortNodeName_AlterRoleCommandsBlock),
        386 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::AlterAnnotationValueStmt),
        387 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::AlterConcreteConstraintStmt),
        388 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::AlterExtending),
        389 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::CreateAnnotationValueStmt),
        390 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::CreateConcreteConstraintStmt),
        391 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::DropAnnotationValueStmt),
        392 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::DropConcreteConstraintStmt),
        393 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::RenameStmt),
        394 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::ResetFieldStmt),
        395 => Reduction::AlterScalarTypeCommand(AlterScalarTypeCommand::SetFieldStmt),
        396 => Reduction::AlterScalarTypeCommandsBlock(AlterScalarTypeCommandsBlock::AlterScalarTypeCommand),
        397 => Reduction::AlterScalarTypeCommandsBlock(AlterScalarTypeCommandsBlock::LBRACE_AlterScalarTypeCommandsList_OptSemicolons_RBRACE),
        398 => Reduction::AlterScalarTypeCommandsBlock(AlterScalarTypeCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        399 => Reduction::AlterScalarTypeCommandsBlock(AlterScalarTypeCommandsBlock::LBRACE_Semicolons_AlterScalarTypeCommandsList_OptSemicolons_RBRACE),
        400 => Reduction::AlterScalarTypeCommandsList(AlterScalarTypeCommandsList::AlterScalarTypeCommand),
        401 => Reduction::AlterScalarTypeCommandsList(AlterScalarTypeCommandsList::AlterScalarTypeCommandsList_Semicolons_AlterScalarTypeCommand),
        402 => Reduction::AlterScalarTypeStmt(AlterScalarTypeStmt::ALTER_SCALAR_TYPE_NodeName_AlterScalarTypeCommandsBlock),
        403 => Reduction::AlterSimpleExtending(AlterSimpleExtending::AlterAbstract),
        404 => Reduction::AlterSimpleExtending(AlterSimpleExtending::DROP_EXTENDING_SimpleTypeNameList),
        405 => Reduction::AlterSimpleExtending(AlterSimpleExtending::EXTENDING_SimpleTypeNameList_OptPosition),
        406 => Reduction::AlterTriggerCommand(AlterTriggerCommand::AccessWhenStmt),
        407 => Reduction::AlterTriggerCommand(AlterTriggerCommand::AlterAnnotationValueStmt),
        408 => Reduction::AlterTriggerCommand(AlterTriggerCommand::CreateAnnotationValueStmt),
        409 => Reduction::AlterTriggerCommand(AlterTriggerCommand::DropAnnotationValueStmt),
        410 => Reduction::AlterTriggerCommand(AlterTriggerCommand::RenameStmt),
        411 => Reduction::AlterTriggerCommand(AlterTriggerCommand::ResetFieldStmt),
        412 => Reduction::AlterTriggerCommand(AlterTriggerCommand::SetFieldStmt),
        413 => Reduction::AlterTriggerCommand(AlterTriggerCommand::UsingStmt),
        414 => Reduction::AlterTriggerCommandsBlock(AlterTriggerCommandsBlock::AlterTriggerCommand),
        415 => Reduction::AlterTriggerCommandsBlock(AlterTriggerCommandsBlock::LBRACE_AlterTriggerCommandsList_OptSemicolons_RBRACE),
        416 => Reduction::AlterTriggerCommandsBlock(AlterTriggerCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        417 => Reduction::AlterTriggerCommandsBlock(AlterTriggerCommandsBlock::LBRACE_Semicolons_AlterTriggerCommandsList_OptSemicolons_RBRACE),
        418 => Reduction::AlterTriggerCommandsList(AlterTriggerCommandsList::AlterTriggerCommand),
        419 => Reduction::AlterTriggerCommandsList(AlterTriggerCommandsList::AlterTriggerCommandsList_Semicolons_AlterTriggerCommand),
        420 => Reduction::AlterTriggerStmt(AlterTriggerStmt::ALTER_TRIGGER_UnqualifiedPointerName_AlterTriggerCommandsBlock),
        421 => Reduction::AnalyzeStmt(AnalyzeStmt::ANALYZE_ExprStmt),
        422 => Reduction::AnalyzeStmt(AnalyzeStmt::ANALYZE_NamedTuple_ExprStmt),
        423 => Reduction::AnnotationDeclaration(AnnotationDeclaration::ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock),
        424 => Reduction::AnnotationDeclaration(AnnotationDeclaration::ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock),
        425 => Reduction::AnnotationDeclarationShort(AnnotationDeclarationShort::ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple),
        426 => Reduction::AnnotationDeclarationShort(AnnotationDeclarationShort::ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple),
        427 => Reduction::AnyIdentifier(AnyIdentifier::PtrIdentifier),
        428 => Reduction::AnyIdentifier(AnyIdentifier::ReservedKeyword),
        429 => Reduction::AnyNodeName(AnyNodeName::AnyIdentifier),
        430 => Reduction::AtomicExpr(AtomicExpr::AtomicPath),
        431 => Reduction::AtomicExpr(AtomicExpr::BaseAtomicExpr),
        432 => Reduction::AtomicExpr(AtomicExpr::LANGBRACKET_FullTypeExpr_RANGBRACKET_AtomicExpr_P_TYPECAST),
        433 => Reduction::AtomicPath(AtomicPath::AtomicExpr_PathStep_P_DOT),
        434 => Reduction::BaseAtomicExpr(BaseAtomicExpr::Collection),
        435 => Reduction::BaseAtomicExpr(BaseAtomicExpr::Constant),
        436 => Reduction::BaseAtomicExpr(BaseAtomicExpr::DUNDERDEFAULT),
        437 => Reduction::BaseAtomicExpr(BaseAtomicExpr::DUNDERNEW),
        438 => Reduction::BaseAtomicExpr(BaseAtomicExpr::DUNDEROLD),
        439 => Reduction::BaseAtomicExpr(BaseAtomicExpr::DUNDERSOURCE),
        440 => Reduction::BaseAtomicExpr(BaseAtomicExpr::DUNDERSPECIFIED),
        441 => Reduction::BaseAtomicExpr(BaseAtomicExpr::DUNDERSUBJECT),
        442 => Reduction::BaseAtomicExpr(BaseAtomicExpr::FreeShape),
        443 => Reduction::BaseAtomicExpr(BaseAtomicExpr::FuncExpr),
        444 => Reduction::BaseAtomicExpr(BaseAtomicExpr::NamedTuple),
        445 => Reduction::BaseAtomicExpr(BaseAtomicExpr::NodeName_P_DOT),
        446 => Reduction::BaseAtomicExpr(BaseAtomicExpr::ParenExpr_P_UMINUS),
        447 => Reduction::BaseAtomicExpr(BaseAtomicExpr::PathStep_P_DOT),
        448 => Reduction::BaseAtomicExpr(BaseAtomicExpr::Set),
        449 => Reduction::BaseAtomicExpr(BaseAtomicExpr::Tuple),
        450 => Reduction::BaseBooleanConstant(BaseBooleanConstant::FALSE),
        451 => Reduction::BaseBooleanConstant(BaseBooleanConstant::TRUE),
        452 => Reduction::BaseBytesConstant(BaseBytesConstant::BCONST),
        453 => Reduction::BaseName(BaseName::Identifier),
        454 => Reduction::BaseName(BaseName::QualifiedName),
        455 => Reduction::BaseNumberConstant(BaseNumberConstant::FCONST),
        456 => Reduction::BaseNumberConstant(BaseNumberConstant::ICONST),
        457 => Reduction::BaseNumberConstant(BaseNumberConstant::NFCONST),
        458 => Reduction::BaseNumberConstant(BaseNumberConstant::NICONST),
        459 => Reduction::BaseStringConstant(BaseStringConstant::SCONST),
        460 => Reduction::BranchOptions(BranchOptions::FORCE),
        461 => Reduction::BranchOptions(BranchOptions::epsilon),
        462 => Reduction::BranchStmt(BranchStmt::AlterBranchStmt),
        463 => Reduction::BranchStmt(BranchStmt::CreateBranchStmt),
        464 => Reduction::BranchStmt(BranchStmt::DropBranchStmt),
        465 => Reduction::ByClause(ByClause::BY_GroupingElementList),
        466 => Reduction::CastAllowedUse(CastAllowedUse::ALLOW_ASSIGNMENT),
        467 => Reduction::CastAllowedUse(CastAllowedUse::ALLOW_IMPLICIT),
        468 => Reduction::CastCode(CastCode::USING_Identifier_BaseStringConstant),
        469 => Reduction::CastCode(CastCode::USING_Identifier_CAST),
        470 => Reduction::CastCode(CastCode::USING_Identifier_EXPRESSION),
        471 => Reduction::CastCode(CastCode::USING_Identifier_FUNCTION_BaseStringConstant),
        472 => Reduction::Collection(Collection::LBRACKET_OptExprList_RBRACKET),
        473 => Reduction::CollectionTypeName(CollectionTypeName::NodeName_LANGBRACKET_RANGBRACKET),
        474 => Reduction::CollectionTypeName(CollectionTypeName::NodeName_LANGBRACKET_SubtypeList_RANGBRACKET),
        475 => Reduction::ColonedIdents(ColonedIdents::AnyIdentifier),
        476 => Reduction::ColonedIdents(ColonedIdents::ColonedIdents_DOUBLECOLON_AnyIdentifier),
        477 => Reduction::CommitMigrationStmt(CommitMigrationStmt::COMMIT_MIGRATION),
        478 => Reduction::CommitMigrationStmt(CommitMigrationStmt::COMMIT_MIGRATION_REWRITE),
        479 => Reduction::CompareOp(CompareOp::DISTINCTFROM_P_COMPARE_OP),
        480 => Reduction::CompareOp(CompareOp::EQUALS_P_COMPARE_OP),
        481 => Reduction::CompareOp(CompareOp::GREATEREQ_P_COMPARE_OP),
        482 => Reduction::CompareOp(CompareOp::LANGBRACKET_P_COMPARE_OP),
        483 => Reduction::CompareOp(CompareOp::LESSEQ_P_COMPARE_OP),
        484 => Reduction::CompareOp(CompareOp::NOTDISTINCTFROM_P_COMPARE_OP),
        485 => Reduction::CompareOp(CompareOp::NOTEQ_P_COMPARE_OP),
        486 => Reduction::CompareOp(CompareOp::RANGBRACKET_P_COMPARE_OP),
        487 => Reduction::ComputableShapePointer(ComputableShapePointer::MULTI_SimpleShapePointer_ASSIGN_Expr),
        488 => Reduction::ComputableShapePointer(ComputableShapePointer::OPTIONAL_MULTI_SimpleShapePointer_ASSIGN_Expr),
        489 => Reduction::ComputableShapePointer(ComputableShapePointer::OPTIONAL_SINGLE_SimpleShapePointer_ASSIGN_Expr),
        490 => Reduction::ComputableShapePointer(ComputableShapePointer::OPTIONAL_SimpleShapePointer_ASSIGN_Expr),
        491 => Reduction::ComputableShapePointer(ComputableShapePointer::REQUIRED_MULTI_SimpleShapePointer_ASSIGN_Expr),
        492 => Reduction::ComputableShapePointer(ComputableShapePointer::REQUIRED_SINGLE_SimpleShapePointer_ASSIGN_Expr),
        493 => Reduction::ComputableShapePointer(ComputableShapePointer::REQUIRED_SimpleShapePointer_ASSIGN_Expr),
        494 => Reduction::ComputableShapePointer(ComputableShapePointer::SINGLE_SimpleShapePointer_ASSIGN_Expr),
        495 => Reduction::ComputableShapePointer(ComputableShapePointer::SimpleShapePointer_ADDASSIGN_Expr),
        496 => Reduction::ComputableShapePointer(ComputableShapePointer::SimpleShapePointer_ASSIGN_Expr),
        497 => Reduction::ComputableShapePointer(ComputableShapePointer::SimpleShapePointer_REMASSIGN_Expr),
        498 => Reduction::ConcreteConstraintBlock(ConcreteConstraintBlock::CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock),
        499 => Reduction::ConcreteConstraintBlock(ConcreteConstraintBlock::DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock),
        500 => Reduction::ConcreteConstraintShort(ConcreteConstraintShort::CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr),
        501 => Reduction::ConcreteConstraintShort(ConcreteConstraintShort::DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr),
        502 => Reduction::ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock::DEFERRED_INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock),
        503 => Reduction::ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock::INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock),
        504 => Reduction::ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock::DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock),
        505 => Reduction::ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock::DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock),
        506 => Reduction::ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock::INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock),
        507 => Reduction::ConcreteIndexDeclarationBlock(ConcreteIndexDeclarationBlock::INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock),
        508 => Reduction::ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort::DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr),
        509 => Reduction::ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort::DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr),
        510 => Reduction::ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort::INDEX_NodeName_OnExpr_OptExceptExpr),
        511 => Reduction::ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort::INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr),
        512 => Reduction::ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort::DEFERRED_INDEX_OnExpr_OptExceptExpr),
        513 => Reduction::ConcreteIndexDeclarationShort(ConcreteIndexDeclarationShort::INDEX_OnExpr_OptExceptExpr),
        514 => Reduction::ConcreteLinkBlock(ConcreteLinkBlock::OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        515 => Reduction::ConcreteLinkBlock(ConcreteLinkBlock::OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        516 => Reduction::ConcreteLinkBlock(ConcreteLinkBlock::LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        517 => Reduction::ConcreteLinkBlock(ConcreteLinkBlock::PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        518 => Reduction::ConcreteLinkShort(ConcreteLinkShort::LINK_PathNodeName_ASSIGN_Expr),
        519 => Reduction::ConcreteLinkShort(ConcreteLinkShort::OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget),
        520 => Reduction::ConcreteLinkShort(ConcreteLinkShort::OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget),
        521 => Reduction::ConcreteLinkShort(ConcreteLinkShort::PtrQuals_LINK_PathNodeName_ASSIGN_Expr),
        522 => Reduction::ConcreteLinkShort(ConcreteLinkShort::LINK_PathNodeName_OptExtendingSimple_PtrTarget),
        523 => Reduction::ConcreteLinkShort(ConcreteLinkShort::PtrQuals_LINK_PathNodeName_OptExtendingSimple_PtrTarget),
        524 => Reduction::ConcretePropertyBlock(ConcretePropertyBlock::OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock),
        525 => Reduction::ConcretePropertyBlock(ConcretePropertyBlock::OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock),
        526 => Reduction::ConcretePropertyBlock(ConcretePropertyBlock::PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock),
        527 => Reduction::ConcretePropertyBlock(ConcretePropertyBlock::PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock),
        528 => Reduction::ConcretePropertyShort(ConcretePropertyShort::PROPERTY_PathNodeName_ASSIGN_Expr),
        529 => Reduction::ConcretePropertyShort(ConcretePropertyShort::OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget),
        530 => Reduction::ConcretePropertyShort(ConcretePropertyShort::OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget),
        531 => Reduction::ConcretePropertyShort(ConcretePropertyShort::PtrQuals_PROPERTY_PathNodeName_ASSIGN_Expr),
        532 => Reduction::ConcretePropertyShort(ConcretePropertyShort::PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget),
        533 => Reduction::ConcretePropertyShort(ConcretePropertyShort::PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget),
        534 => Reduction::ConcreteUnknownPointerBlock(ConcreteUnknownPointerBlock::OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        535 => Reduction::ConcreteUnknownPointerBlock(ConcreteUnknownPointerBlock::OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        536 => Reduction::ConcreteUnknownPointerBlock(ConcreteUnknownPointerBlock::PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        537 => Reduction::ConcreteUnknownPointerBlock(ConcreteUnknownPointerBlock::PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock),
        538 => Reduction::ConcreteUnknownPointerObjectShort(ConcreteUnknownPointerObjectShort::PathNodeName_ASSIGN_Expr),
        539 => Reduction::ConcreteUnknownPointerObjectShort(ConcreteUnknownPointerObjectShort::PtrQuals_PathNodeName_ASSIGN_Expr),
        540 => Reduction::ConcreteUnknownPointerShort(ConcreteUnknownPointerShort::OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget),
        541 => Reduction::ConcreteUnknownPointerShort(ConcreteUnknownPointerShort::OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget),
        542 => Reduction::ConcreteUnknownPointerShort(ConcreteUnknownPointerShort::PathNodeName_OptExtendingSimple_PtrTarget),
        543 => Reduction::ConcreteUnknownPointerShort(ConcreteUnknownPointerShort::PtrQuals_PathNodeName_OptExtendingSimple_PtrTarget),
        544 => Reduction::ConfigOp(ConfigOp::INSERT_NodeName_Shape),
        545 => Reduction::ConfigOp(ConfigOp::RESET_NodeName_OptFilterClause),
        546 => Reduction::ConfigOp(ConfigOp::SET_NodeName_ASSIGN_Expr),
        547 => Reduction::ConfigScope(ConfigScope::CURRENT_BRANCH),
        548 => Reduction::ConfigScope(ConfigScope::CURRENT_DATABASE),
        549 => Reduction::ConfigScope(ConfigScope::INSTANCE),
        550 => Reduction::ConfigScope(ConfigScope::SESSION),
        551 => Reduction::ConfigScope(ConfigScope::SYSTEM),
        552 => Reduction::ConfigStmt(ConfigStmt::CONFIGURE_BRANCH_ConfigOp),
        553 => Reduction::ConfigStmt(ConfigStmt::CONFIGURE_ConfigScope_ConfigOp),
        554 => Reduction::ConfigStmt(ConfigStmt::CONFIGURE_DATABASE_ConfigOp),
        555 => Reduction::ConfigStmt(ConfigStmt::RESET_GLOBAL_NodeName),
        556 => Reduction::ConfigStmt(ConfigStmt::SET_GLOBAL_NodeName_ASSIGN_Expr),
        557 => Reduction::Constant(Constant::BaseBooleanConstant),
        558 => Reduction::Constant(Constant::BaseBytesConstant),
        559 => Reduction::Constant(Constant::BaseNumberConstant),
        560 => Reduction::Constant(Constant::BaseStringConstant),
        561 => Reduction::Constant(Constant::PARAMETER),
        562 => Reduction::Constant(Constant::PARAMETERANDTYPE),
        563 => Reduction::ConstraintDeclaration(ConstraintDeclaration::ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock),
        564 => Reduction::ConstraintDeclaration(ConstraintDeclaration::ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock),
        565 => Reduction::ConstraintDeclarationShort(ConstraintDeclarationShort::ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple),
        566 => Reduction::ConstraintDeclarationShort(ConstraintDeclarationShort::ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple),
        567 => Reduction::CreateAccessPolicyCommand(CreateAccessPolicyCommand::CreateAnnotationValueStmt),
        568 => Reduction::CreateAccessPolicyCommand(CreateAccessPolicyCommand::SetFieldStmt),
        569 => Reduction::CreateAccessPolicyCommandsBlock(CreateAccessPolicyCommandsBlock::LBRACE_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE),
        570 => Reduction::CreateAccessPolicyCommandsBlock(CreateAccessPolicyCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        571 => Reduction::CreateAccessPolicyCommandsBlock(CreateAccessPolicyCommandsBlock::LBRACE_Semicolons_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE),
        572 => Reduction::CreateAccessPolicyCommandsList(CreateAccessPolicyCommandsList::CreateAccessPolicyCommand),
        573 => Reduction::CreateAccessPolicyCommandsList(CreateAccessPolicyCommandsList::CreateAccessPolicyCommandsList_Semicolons_CreateAccessPolicyCommand),
        574 => Reduction::CreateAccessPolicySDLCommandFull(CreateAccessPolicySDLCommandFull::CreateAccessPolicySDLCommandBlock),
        575 => Reduction::CreateAccessPolicySDLCommandFull(CreateAccessPolicySDLCommandFull::CreateAccessPolicySDLCommandShort_SEMICOLON),
        576 => Reduction::CreateAccessPolicySDLCommandShort(CreateAccessPolicySDLCommandShort::SetAnnotation),
        577 => Reduction::CreateAccessPolicySDLCommandShort(CreateAccessPolicySDLCommandShort::SetField),
        578 => Reduction::CreateAccessPolicySDLCommandsBlock(CreateAccessPolicySDLCommandsBlock::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE),
        579 => Reduction::CreateAccessPolicySDLCommandsBlock(CreateAccessPolicySDLCommandsBlock::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE),
        580 => Reduction::CreateAccessPolicySDLCommandsBlock(CreateAccessPolicySDLCommandsBlock::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_RBRACE),
        581 => Reduction::CreateAccessPolicySDLCommandsBlock(CreateAccessPolicySDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        582 => Reduction::CreateAccessPolicySDLCommandsList(CreateAccessPolicySDLCommandsList::CreateAccessPolicySDLCommandFull),
        583 => Reduction::CreateAccessPolicySDLCommandsList(CreateAccessPolicySDLCommandsList::CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandFull),
        584 => Reduction::CreateAccessPolicyStmt(CreateAccessPolicyStmt::CREATE_ACCESS_POLICY_UnqualifiedPointerName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_OptCreateAccessPolicyCommandsBlock),
        585 => Reduction::CreateAliasCommand(CreateAliasCommand::AlterAnnotationValueStmt),
        586 => Reduction::CreateAliasCommand(CreateAliasCommand::CreateAnnotationValueStmt),
        587 => Reduction::CreateAliasCommand(CreateAliasCommand::SetFieldStmt),
        588 => Reduction::CreateAliasCommand(CreateAliasCommand::UsingStmt),
        589 => Reduction::CreateAliasCommandsBlock(CreateAliasCommandsBlock::CreateAliasCommand),
        590 => Reduction::CreateAliasCommandsBlock(CreateAliasCommandsBlock::LBRACE_CreateAliasCommandsList_OptSemicolons_RBRACE),
        591 => Reduction::CreateAliasCommandsBlock(CreateAliasCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        592 => Reduction::CreateAliasCommandsBlock(CreateAliasCommandsBlock::LBRACE_Semicolons_CreateAliasCommandsList_OptSemicolons_RBRACE),
        593 => Reduction::CreateAliasCommandsList(CreateAliasCommandsList::CreateAliasCommand),
        594 => Reduction::CreateAliasCommandsList(CreateAliasCommandsList::CreateAliasCommandsList_Semicolons_CreateAliasCommand),
        595 => Reduction::CreateAliasSDLCommandFull(CreateAliasSDLCommandFull::CreateAliasSDLCommandBlock),
        596 => Reduction::CreateAliasSDLCommandFull(CreateAliasSDLCommandFull::CreateAliasSDLCommandShort_SEMICOLON),
        597 => Reduction::CreateAliasSDLCommandShort(CreateAliasSDLCommandShort::SetAnnotation),
        598 => Reduction::CreateAliasSDLCommandShort(CreateAliasSDLCommandShort::SetField),
        599 => Reduction::CreateAliasSDLCommandShort(CreateAliasSDLCommandShort::Using),
        600 => Reduction::CreateAliasSDLCommandsBlock(CreateAliasSDLCommandsBlock::LBRACE_OptSemicolons_CreateAliasSDLCommandShort_RBRACE),
        601 => Reduction::CreateAliasSDLCommandsBlock(CreateAliasSDLCommandsBlock::LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandShort_RBRACE),
        602 => Reduction::CreateAliasSDLCommandsBlock(CreateAliasSDLCommandsBlock::LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_RBRACE),
        603 => Reduction::CreateAliasSDLCommandsBlock(CreateAliasSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        604 => Reduction::CreateAliasSDLCommandsList(CreateAliasSDLCommandsList::CreateAliasSDLCommandFull),
        605 => Reduction::CreateAliasSDLCommandsList(CreateAliasSDLCommandsList::CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandFull),
        606 => Reduction::CreateAliasSingleSDLCommandBlock(CreateAliasSingleSDLCommandBlock::CreateAliasSDLCommandBlock),
        607 => Reduction::CreateAliasSingleSDLCommandBlock(CreateAliasSingleSDLCommandBlock::CreateAliasSDLCommandShort),
        608 => Reduction::CreateAliasStmt(CreateAliasStmt::CREATE_ALIAS_NodeName_CreateAliasCommandsBlock),
        609 => Reduction::CreateAliasStmt(CreateAliasStmt::CREATE_ALIAS_NodeName_ASSIGN_Expr),
        610 => Reduction::CreateAnnotationCommand(CreateAnnotationCommand::CreateAnnotationValueStmt),
        611 => Reduction::CreateAnnotationCommandsBlock(CreateAnnotationCommandsBlock::LBRACE_CreateAnnotationCommandsList_OptSemicolons_RBRACE),
        612 => Reduction::CreateAnnotationCommandsBlock(CreateAnnotationCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        613 => Reduction::CreateAnnotationCommandsBlock(CreateAnnotationCommandsBlock::LBRACE_Semicolons_CreateAnnotationCommandsList_OptSemicolons_RBRACE),
        614 => Reduction::CreateAnnotationCommandsList(CreateAnnotationCommandsList::CreateAnnotationCommand),
        615 => Reduction::CreateAnnotationCommandsList(CreateAnnotationCommandsList::CreateAnnotationCommandsList_Semicolons_CreateAnnotationCommand),
        616 => Reduction::CreateAnnotationStmt(CreateAnnotationStmt::CREATE_ABSTRACT_ANNOTATION_NodeName_OptCreateAnnotationCommandsBlock),
        617 => Reduction::CreateAnnotationStmt(CreateAnnotationStmt::CREATE_ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptCreateCommandsBlock),
        618 => Reduction::CreateAnnotationValueStmt(CreateAnnotationValueStmt::CREATE_ANNOTATION_NodeName_ASSIGN_Expr),
        619 => Reduction::CreateBranchStmt(CreateBranchStmt::CREATE_EMPTY_BRANCH_DatabaseName),
        620 => Reduction::CreateBranchStmt(CreateBranchStmt::CREATE_DATA_BRANCH_DatabaseName_FROM_DatabaseName),
        621 => Reduction::CreateBranchStmt(CreateBranchStmt::CREATE_SCHEMA_BRANCH_DatabaseName_FROM_DatabaseName),
        622 => Reduction::CreateBranchStmt(CreateBranchStmt::CREATE_TEMPLATE_BRANCH_DatabaseName_FROM_DatabaseName),
        623 => Reduction::CreateCastCommand(CreateCastCommand::AlterAnnotationValueStmt),
        624 => Reduction::CreateCastCommand(CreateCastCommand::CastAllowedUse),
        625 => Reduction::CreateCastCommand(CreateCastCommand::CastCode),
        626 => Reduction::CreateCastCommand(CreateCastCommand::CreateAnnotationValueStmt),
        627 => Reduction::CreateCastCommand(CreateCastCommand::SetFieldStmt),
        628 => Reduction::CreateCastCommandsBlock(CreateCastCommandsBlock::CreateCastCommand),
        629 => Reduction::CreateCastCommandsBlock(CreateCastCommandsBlock::LBRACE_CreateCastCommandsList_OptSemicolons_RBRACE),
        630 => Reduction::CreateCastCommandsBlock(CreateCastCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        631 => Reduction::CreateCastCommandsBlock(CreateCastCommandsBlock::LBRACE_Semicolons_CreateCastCommandsList_OptSemicolons_RBRACE),
        632 => Reduction::CreateCastCommandsList(CreateCastCommandsList::CreateCastCommand),
        633 => Reduction::CreateCastCommandsList(CreateCastCommandsList::CreateCastCommandsList_Semicolons_CreateCastCommand),
        634 => Reduction::CreateCastStmt(CreateCastStmt::CREATE_CAST_FROM_TypeName_TO_TypeName_CreateCastCommandsBlock),
        635 => Reduction::CreateCommand(CreateCommand::AlterAnnotationValueStmt),
        636 => Reduction::CreateCommand(CreateCommand::CreateAnnotationValueStmt),
        637 => Reduction::CreateCommand(CreateCommand::SetFieldStmt),
        638 => Reduction::CreateCommand(CreateCommand::UsingStmt),
        639 => Reduction::CreateCommandsBlock(CreateCommandsBlock::LBRACE_CreateCommandsList_OptSemicolons_RBRACE),
        640 => Reduction::CreateCommandsBlock(CreateCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        641 => Reduction::CreateCommandsBlock(CreateCommandsBlock::LBRACE_Semicolons_CreateCommandsList_OptSemicolons_RBRACE),
        642 => Reduction::CreateCommandsList(CreateCommandsList::CreateCommand),
        643 => Reduction::CreateCommandsList(CreateCommandsList::CreateCommandsList_Semicolons_CreateCommand),
        644 => Reduction::CreateConcreteConstraintStmt(CreateConcreteConstraintStmt::CREATE_OptDelegated_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_OptCreateCommandsBlock),
        645 => Reduction::CreateConcreteIndexSDLCommandFull(CreateConcreteIndexSDLCommandFull::CreateConcreteIndexSDLCommandBlock),
        646 => Reduction::CreateConcreteIndexSDLCommandFull(CreateConcreteIndexSDLCommandFull::CreateConcreteIndexSDLCommandShort_SEMICOLON),
        647 => Reduction::CreateConcreteIndexSDLCommandShort(CreateConcreteIndexSDLCommandShort::SetAnnotation),
        648 => Reduction::CreateConcreteIndexSDLCommandShort(CreateConcreteIndexSDLCommandShort::SetField),
        649 => Reduction::CreateConcreteIndexSDLCommandsBlock(CreateConcreteIndexSDLCommandsBlock::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE),
        650 => Reduction::CreateConcreteIndexSDLCommandsBlock(CreateConcreteIndexSDLCommandsBlock::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE),
        651 => Reduction::CreateConcreteIndexSDLCommandsBlock(CreateConcreteIndexSDLCommandsBlock::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_RBRACE),
        652 => Reduction::CreateConcreteIndexSDLCommandsBlock(CreateConcreteIndexSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        653 => Reduction::CreateConcreteIndexSDLCommandsList(CreateConcreteIndexSDLCommandsList::CreateConcreteIndexSDLCommandFull),
        654 => Reduction::CreateConcreteIndexSDLCommandsList(CreateConcreteIndexSDLCommandsList::CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandFull),
        655 => Reduction::CreateConcreteIndexStmt(CreateConcreteIndexStmt::CREATE_OptDeferred_INDEX_OnExpr_OptExceptExpr_OptCreateCommandsBlock),
        656 => Reduction::CreateConcreteIndexStmt(CreateConcreteIndexStmt::CREATE_OptDeferred_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptCreateCommandsBlock),
        657 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::AlterAnnotationValueStmt),
        658 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::CreateAnnotationValueStmt),
        659 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::CreateConcreteConstraintStmt),
        660 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::CreateConcreteIndexStmt),
        661 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::CreateConcretePropertyStmt),
        662 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::CreateRewriteStmt),
        663 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::CreateSimpleExtending),
        664 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::OnSourceDeleteStmt),
        665 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::OnTargetDeleteStmt),
        666 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::SetFieldStmt),
        667 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::SetRequiredInCreateStmt),
        668 => Reduction::CreateConcreteLinkCommand(CreateConcreteLinkCommand::UsingStmt),
        669 => Reduction::CreateConcreteLinkCommandsBlock(CreateConcreteLinkCommandsBlock::LBRACE_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE),
        670 => Reduction::CreateConcreteLinkCommandsBlock(CreateConcreteLinkCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        671 => Reduction::CreateConcreteLinkCommandsBlock(CreateConcreteLinkCommandsBlock::LBRACE_Semicolons_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE),
        672 => Reduction::CreateConcreteLinkCommandsList(CreateConcreteLinkCommandsList::CreateConcreteLinkCommand),
        673 => Reduction::CreateConcreteLinkCommandsList(CreateConcreteLinkCommandsList::CreateConcreteLinkCommandsList_Semicolons_CreateConcreteLinkCommand),
        674 => Reduction::CreateConcreteLinkSDLCommandBlock(CreateConcreteLinkSDLCommandBlock::ConcreteConstraintBlock),
        675 => Reduction::CreateConcreteLinkSDLCommandBlock(CreateConcreteLinkSDLCommandBlock::ConcreteIndexDeclarationBlock),
        676 => Reduction::CreateConcreteLinkSDLCommandBlock(CreateConcreteLinkSDLCommandBlock::ConcretePropertyBlock),
        677 => Reduction::CreateConcreteLinkSDLCommandBlock(CreateConcreteLinkSDLCommandBlock::ConcreteUnknownPointerBlock),
        678 => Reduction::CreateConcreteLinkSDLCommandBlock(CreateConcreteLinkSDLCommandBlock::RewriteDeclarationBlock),
        679 => Reduction::CreateConcreteLinkSDLCommandFull(CreateConcreteLinkSDLCommandFull::CreateConcreteLinkSDLCommandBlock),
        680 => Reduction::CreateConcreteLinkSDLCommandFull(CreateConcreteLinkSDLCommandFull::CreateConcreteLinkSDLCommandShort_SEMICOLON),
        681 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::ConcreteConstraintShort),
        682 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::ConcreteIndexDeclarationShort),
        683 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::ConcretePropertyShort),
        684 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::ConcreteUnknownPointerShort),
        685 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::CreateSimpleExtending),
        686 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::OnSourceDeleteStmt),
        687 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::OnTargetDeleteStmt),
        688 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::RewriteDeclarationShort),
        689 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::SetAnnotation),
        690 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::SetField),
        691 => Reduction::CreateConcreteLinkSDLCommandShort(CreateConcreteLinkSDLCommandShort::Using),
        692 => Reduction::CreateConcreteLinkSDLCommandsBlock(CreateConcreteLinkSDLCommandsBlock::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE),
        693 => Reduction::CreateConcreteLinkSDLCommandsBlock(CreateConcreteLinkSDLCommandsBlock::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE),
        694 => Reduction::CreateConcreteLinkSDLCommandsBlock(CreateConcreteLinkSDLCommandsBlock::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_RBRACE),
        695 => Reduction::CreateConcreteLinkSDLCommandsBlock(CreateConcreteLinkSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        696 => Reduction::CreateConcreteLinkSDLCommandsList(CreateConcreteLinkSDLCommandsList::CreateConcreteLinkSDLCommandFull),
        697 => Reduction::CreateConcreteLinkSDLCommandsList(CreateConcreteLinkSDLCommandsList::CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandFull),
        698 => Reduction::CreateConcreteLinkStmt(CreateConcreteLinkStmt::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_ASSIGN_Expr),
        699 => Reduction::CreateConcreteLinkStmt(CreateConcreteLinkStmt::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptCreateConcreteLinkCommandsBlock),
        700 => Reduction::CreateConcreteLinkStmt(CreateConcreteLinkStmt::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcreteLinkCommandsBlock),
        701 => Reduction::CreateConcreteLinkStmt(CreateConcreteLinkStmt::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcreteLinkCommandsBlock),
        702 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::AlterAnnotationValueStmt),
        703 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::CreateAnnotationValueStmt),
        704 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::CreateConcreteConstraintStmt),
        705 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::CreateRewriteStmt),
        706 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::CreateSimpleExtending),
        707 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::SetFieldStmt),
        708 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::SetRequiredInCreateStmt),
        709 => Reduction::CreateConcretePropertyCommand(CreateConcretePropertyCommand::UsingStmt),
        710 => Reduction::CreateConcretePropertyCommandsBlock(CreateConcretePropertyCommandsBlock::LBRACE_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE),
        711 => Reduction::CreateConcretePropertyCommandsBlock(CreateConcretePropertyCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        712 => Reduction::CreateConcretePropertyCommandsBlock(CreateConcretePropertyCommandsBlock::LBRACE_Semicolons_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE),
        713 => Reduction::CreateConcretePropertyCommandsList(CreateConcretePropertyCommandsList::CreateConcretePropertyCommand),
        714 => Reduction::CreateConcretePropertyCommandsList(CreateConcretePropertyCommandsList::CreateConcretePropertyCommandsList_Semicolons_CreateConcretePropertyCommand),
        715 => Reduction::CreateConcretePropertySDLCommandBlock(CreateConcretePropertySDLCommandBlock::ConcreteConstraintBlock),
        716 => Reduction::CreateConcretePropertySDLCommandBlock(CreateConcretePropertySDLCommandBlock::RewriteDeclarationBlock),
        717 => Reduction::CreateConcretePropertySDLCommandFull(CreateConcretePropertySDLCommandFull::CreateConcretePropertySDLCommandBlock),
        718 => Reduction::CreateConcretePropertySDLCommandFull(CreateConcretePropertySDLCommandFull::CreateConcretePropertySDLCommandShort_SEMICOLON),
        719 => Reduction::CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort::ConcreteConstraintShort),
        720 => Reduction::CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort::CreateSimpleExtending),
        721 => Reduction::CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort::RewriteDeclarationShort),
        722 => Reduction::CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort::SetAnnotation),
        723 => Reduction::CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort::SetField),
        724 => Reduction::CreateConcretePropertySDLCommandShort(CreateConcretePropertySDLCommandShort::Using),
        725 => Reduction::CreateConcretePropertySDLCommandsBlock(CreateConcretePropertySDLCommandsBlock::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE),
        726 => Reduction::CreateConcretePropertySDLCommandsBlock(CreateConcretePropertySDLCommandsBlock::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE),
        727 => Reduction::CreateConcretePropertySDLCommandsBlock(CreateConcretePropertySDLCommandsBlock::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_RBRACE),
        728 => Reduction::CreateConcretePropertySDLCommandsBlock(CreateConcretePropertySDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        729 => Reduction::CreateConcretePropertySDLCommandsList(CreateConcretePropertySDLCommandsList::CreateConcretePropertySDLCommandFull),
        730 => Reduction::CreateConcretePropertySDLCommandsList(CreateConcretePropertySDLCommandsList::CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandFull),
        731 => Reduction::CreateConcretePropertyStmt(CreateConcretePropertyStmt::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_ASSIGN_Expr),
        732 => Reduction::CreateConcretePropertyStmt(CreateConcretePropertyStmt::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptCreateConcretePropertyCommandsBlock),
        733 => Reduction::CreateConcretePropertyStmt(CreateConcretePropertyStmt::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcretePropertyCommandsBlock),
        734 => Reduction::CreateConcretePropertyStmt(CreateConcretePropertyStmt::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcretePropertyCommandsBlock),
        735 => Reduction::CreateConstraintStmt(CreateConstraintStmt::CREATE_ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock),
        736 => Reduction::CreateConstraintStmt(CreateConstraintStmt::CREATE_ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock),
        737 => Reduction::CreateDatabaseCommand(CreateDatabaseCommand::SetFieldStmt),
        738 => Reduction::CreateDatabaseCommandsBlock(CreateDatabaseCommandsBlock::LBRACE_CreateDatabaseCommandsList_OptSemicolons_RBRACE),
        739 => Reduction::CreateDatabaseCommandsBlock(CreateDatabaseCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        740 => Reduction::CreateDatabaseCommandsBlock(CreateDatabaseCommandsBlock::LBRACE_Semicolons_CreateDatabaseCommandsList_OptSemicolons_RBRACE),
        741 => Reduction::CreateDatabaseCommandsList(CreateDatabaseCommandsList::CreateDatabaseCommand),
        742 => Reduction::CreateDatabaseCommandsList(CreateDatabaseCommandsList::CreateDatabaseCommandsList_Semicolons_CreateDatabaseCommand),
        743 => Reduction::CreateDatabaseStmt(CreateDatabaseStmt::CREATE_DATABASE_DatabaseName_FROM_AnyNodeName_OptCreateDatabaseCommandsBlock),
        744 => Reduction::CreateDatabaseStmt(CreateDatabaseStmt::CREATE_DATABASE_DatabaseName_OptCreateDatabaseCommandsBlock),
        745 => Reduction::CreateExtensionCommand(CreateExtensionCommand::SetFieldStmt),
        746 => Reduction::CreateExtensionCommandsBlock(CreateExtensionCommandsBlock::LBRACE_CreateExtensionCommandsList_OptSemicolons_RBRACE),
        747 => Reduction::CreateExtensionCommandsBlock(CreateExtensionCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        748 => Reduction::CreateExtensionCommandsBlock(CreateExtensionCommandsBlock::LBRACE_Semicolons_CreateExtensionCommandsList_OptSemicolons_RBRACE),
        749 => Reduction::CreateExtensionCommandsList(CreateExtensionCommandsList::CreateExtensionCommand),
        750 => Reduction::CreateExtensionCommandsList(CreateExtensionCommandsList::CreateExtensionCommandsList_Semicolons_CreateExtensionCommand),
        751 => Reduction::CreateExtensionPackageCommand(CreateExtensionPackageCommand::NestedQLBlockStmt),
        752 => Reduction::CreateExtensionPackageCommandsBlock(CreateExtensionPackageCommandsBlock::LBRACE_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE),
        753 => Reduction::CreateExtensionPackageCommandsBlock(CreateExtensionPackageCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        754 => Reduction::CreateExtensionPackageCommandsBlock(CreateExtensionPackageCommandsBlock::LBRACE_Semicolons_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE),
        755 => Reduction::CreateExtensionPackageCommandsList(CreateExtensionPackageCommandsList::CreateExtensionPackageCommand),
        756 => Reduction::CreateExtensionPackageCommandsList(CreateExtensionPackageCommandsList::CreateExtensionPackageCommandsList_Semicolons_CreateExtensionPackageCommand),
        757 => Reduction::CreateExtensionPackageStmt(CreateExtensionPackageStmt::CREATE_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion_OptCreateExtensionPackageCommandsBlock),
        758 => Reduction::CreateExtensionStmt(CreateExtensionStmt::CREATE_EXTENSION_ShortNodeName_OptExtensionVersion_OptCreateExtensionCommandsBlock),
        759 => Reduction::CreateFunctionArgs(CreateFunctionArgs::LPAREN_FuncDeclArgs_RPAREN),
        760 => Reduction::CreateFunctionArgs(CreateFunctionArgs::LPAREN_RPAREN),
        761 => Reduction::CreateFunctionCommand(CreateFunctionCommand::AlterAnnotationValueStmt),
        762 => Reduction::CreateFunctionCommand(CreateFunctionCommand::CreateAnnotationValueStmt),
        763 => Reduction::CreateFunctionCommand(CreateFunctionCommand::FromFunction),
        764 => Reduction::CreateFunctionCommand(CreateFunctionCommand::SetFieldStmt),
        765 => Reduction::CreateFunctionCommandsBlock(CreateFunctionCommandsBlock::CreateFunctionCommand),
        766 => Reduction::CreateFunctionCommandsBlock(CreateFunctionCommandsBlock::LBRACE_CreateFunctionCommandsList_OptSemicolons_RBRACE),
        767 => Reduction::CreateFunctionCommandsBlock(CreateFunctionCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        768 => Reduction::CreateFunctionCommandsBlock(CreateFunctionCommandsBlock::LBRACE_Semicolons_CreateFunctionCommandsList_OptSemicolons_RBRACE),
        769 => Reduction::CreateFunctionCommandsList(CreateFunctionCommandsList::CreateFunctionCommand),
        770 => Reduction::CreateFunctionCommandsList(CreateFunctionCommandsList::CreateFunctionCommandsList_Semicolons_CreateFunctionCommand),
        771 => Reduction::CreateFunctionSDLCommandFull(CreateFunctionSDLCommandFull::CreateFunctionSDLCommandBlock),
        772 => Reduction::CreateFunctionSDLCommandFull(CreateFunctionSDLCommandFull::CreateFunctionSDLCommandShort_SEMICOLON),
        773 => Reduction::CreateFunctionSDLCommandShort(CreateFunctionSDLCommandShort::FromFunction),
        774 => Reduction::CreateFunctionSDLCommandShort(CreateFunctionSDLCommandShort::SetAnnotation),
        775 => Reduction::CreateFunctionSDLCommandShort(CreateFunctionSDLCommandShort::SetField),
        776 => Reduction::CreateFunctionSDLCommandsBlock(CreateFunctionSDLCommandsBlock::LBRACE_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE),
        777 => Reduction::CreateFunctionSDLCommandsBlock(CreateFunctionSDLCommandsBlock::LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE),
        778 => Reduction::CreateFunctionSDLCommandsBlock(CreateFunctionSDLCommandsBlock::LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_RBRACE),
        779 => Reduction::CreateFunctionSDLCommandsBlock(CreateFunctionSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        780 => Reduction::CreateFunctionSDLCommandsList(CreateFunctionSDLCommandsList::CreateFunctionSDLCommandFull),
        781 => Reduction::CreateFunctionSDLCommandsList(CreateFunctionSDLCommandsList::CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandFull),
        782 => Reduction::CreateFunctionSingleSDLCommandBlock(CreateFunctionSingleSDLCommandBlock::CreateFunctionSDLCommandBlock),
        783 => Reduction::CreateFunctionSingleSDLCommandBlock(CreateFunctionSingleSDLCommandBlock::CreateFunctionSDLCommandShort),
        784 => Reduction::CreateFunctionStmt(CreateFunctionStmt::CREATE_FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionCommandsBlock),
        785 => Reduction::CreateFutureStmt(CreateFutureStmt::CREATE_FUTURE_ShortNodeName),
        786 => Reduction::CreateGlobalCommand(CreateGlobalCommand::CreateAnnotationValueStmt),
        787 => Reduction::CreateGlobalCommand(CreateGlobalCommand::SetFieldStmt),
        788 => Reduction::CreateGlobalCommand(CreateGlobalCommand::UsingStmt),
        789 => Reduction::CreateGlobalCommandsBlock(CreateGlobalCommandsBlock::LBRACE_CreateGlobalCommandsList_OptSemicolons_RBRACE),
        790 => Reduction::CreateGlobalCommandsBlock(CreateGlobalCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        791 => Reduction::CreateGlobalCommandsBlock(CreateGlobalCommandsBlock::LBRACE_Semicolons_CreateGlobalCommandsList_OptSemicolons_RBRACE),
        792 => Reduction::CreateGlobalCommandsList(CreateGlobalCommandsList::CreateGlobalCommand),
        793 => Reduction::CreateGlobalCommandsList(CreateGlobalCommandsList::CreateGlobalCommandsList_Semicolons_CreateGlobalCommand),
        794 => Reduction::CreateGlobalSDLCommandFull(CreateGlobalSDLCommandFull::CreateGlobalSDLCommandBlock),
        795 => Reduction::CreateGlobalSDLCommandFull(CreateGlobalSDLCommandFull::CreateGlobalSDLCommandShort_SEMICOLON),
        796 => Reduction::CreateGlobalSDLCommandShort(CreateGlobalSDLCommandShort::SetAnnotation),
        797 => Reduction::CreateGlobalSDLCommandShort(CreateGlobalSDLCommandShort::SetField),
        798 => Reduction::CreateGlobalSDLCommandShort(CreateGlobalSDLCommandShort::Using),
        799 => Reduction::CreateGlobalSDLCommandsBlock(CreateGlobalSDLCommandsBlock::LBRACE_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE),
        800 => Reduction::CreateGlobalSDLCommandsBlock(CreateGlobalSDLCommandsBlock::LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE),
        801 => Reduction::CreateGlobalSDLCommandsBlock(CreateGlobalSDLCommandsBlock::LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_RBRACE),
        802 => Reduction::CreateGlobalSDLCommandsBlock(CreateGlobalSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        803 => Reduction::CreateGlobalSDLCommandsList(CreateGlobalSDLCommandsList::CreateGlobalSDLCommandFull),
        804 => Reduction::CreateGlobalSDLCommandsList(CreateGlobalSDLCommandsList::CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandFull),
        805 => Reduction::CreateGlobalStmt(CreateGlobalStmt::CREATE_OptPtrQuals_GLOBAL_NodeName_ASSIGN_Expr),
        806 => Reduction::CreateGlobalStmt(CreateGlobalStmt::CREATE_OptPtrQuals_GLOBAL_NodeName_OptCreateConcretePropertyCommandsBlock),
        807 => Reduction::CreateGlobalStmt(CreateGlobalStmt::CREATE_OptPtrQuals_GLOBAL_NodeName_ARROW_FullTypeExpr_OptCreateGlobalCommandsBlock),
        808 => Reduction::CreateGlobalStmt(CreateGlobalStmt::CREATE_OptPtrQuals_GLOBAL_NodeName_COLON_FullTypeExpr_OptCreateGlobalCommandsBlock),
        809 => Reduction::CreateIndexCommand(CreateIndexCommand::AlterAnnotationValueStmt),
        810 => Reduction::CreateIndexCommand(CreateIndexCommand::CreateAnnotationValueStmt),
        811 => Reduction::CreateIndexCommand(CreateIndexCommand::SetFieldStmt),
        812 => Reduction::CreateIndexCommand(CreateIndexCommand::UsingStmt),
        813 => Reduction::CreateIndexCommandsBlock(CreateIndexCommandsBlock::LBRACE_CreateIndexCommandsList_OptSemicolons_RBRACE),
        814 => Reduction::CreateIndexCommandsBlock(CreateIndexCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        815 => Reduction::CreateIndexCommandsBlock(CreateIndexCommandsBlock::LBRACE_Semicolons_CreateIndexCommandsList_OptSemicolons_RBRACE),
        816 => Reduction::CreateIndexCommandsList(CreateIndexCommandsList::CreateIndexCommand),
        817 => Reduction::CreateIndexCommandsList(CreateIndexCommandsList::CreateIndexCommandsList_Semicolons_CreateIndexCommand),
        818 => Reduction::CreateIndexMatchCommand(CreateIndexMatchCommand::CreateAnnotationValueStmt),
        819 => Reduction::CreateIndexMatchCommandsBlock(CreateIndexMatchCommandsBlock::LBRACE_CreateIndexMatchCommandsList_OptSemicolons_RBRACE),
        820 => Reduction::CreateIndexMatchCommandsBlock(CreateIndexMatchCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        821 => Reduction::CreateIndexMatchCommandsBlock(CreateIndexMatchCommandsBlock::LBRACE_Semicolons_CreateIndexMatchCommandsList_OptSemicolons_RBRACE),
        822 => Reduction::CreateIndexMatchCommandsList(CreateIndexMatchCommandsList::CreateIndexMatchCommand),
        823 => Reduction::CreateIndexMatchCommandsList(CreateIndexMatchCommandsList::CreateIndexMatchCommandsList_Semicolons_CreateIndexMatchCommand),
        824 => Reduction::CreateIndexMatchStmt(CreateIndexMatchStmt::CREATE_INDEX_MATCH_FOR_TypeName_USING_NodeName_OptCreateIndexMatchCommandsBlock),
        825 => Reduction::CreateIndexSDLCommandFull(CreateIndexSDLCommandFull::CreateIndexSDLCommandBlock),
        826 => Reduction::CreateIndexSDLCommandFull(CreateIndexSDLCommandFull::CreateIndexSDLCommandShort_SEMICOLON),
        827 => Reduction::CreateIndexSDLCommandShort(CreateIndexSDLCommandShort::SetAnnotation),
        828 => Reduction::CreateIndexSDLCommandShort(CreateIndexSDLCommandShort::SetField),
        829 => Reduction::CreateIndexSDLCommandShort(CreateIndexSDLCommandShort::Using),
        830 => Reduction::CreateIndexSDLCommandsBlock(CreateIndexSDLCommandsBlock::LBRACE_OptSemicolons_CreateIndexSDLCommandShort_RBRACE),
        831 => Reduction::CreateIndexSDLCommandsBlock(CreateIndexSDLCommandsBlock::LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandShort_RBRACE),
        832 => Reduction::CreateIndexSDLCommandsBlock(CreateIndexSDLCommandsBlock::LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_RBRACE),
        833 => Reduction::CreateIndexSDLCommandsBlock(CreateIndexSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        834 => Reduction::CreateIndexSDLCommandsList(CreateIndexSDLCommandsList::CreateIndexSDLCommandFull),
        835 => Reduction::CreateIndexSDLCommandsList(CreateIndexSDLCommandsList::CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandFull),
        836 => Reduction::CreateIndexStmt(CreateIndexStmt::CREATE_ABSTRACT_INDEX_NodeName_OptExtendingSimple_OptCreateIndexCommandsBlock),
        837 => Reduction::CreateIndexStmt(CreateIndexStmt::CREATE_ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_OptCreateIndexCommandsBlock),
        838 => Reduction::CreateLinkCommand(CreateLinkCommand::AlterAnnotationValueStmt),
        839 => Reduction::CreateLinkCommand(CreateLinkCommand::CreateAnnotationValueStmt),
        840 => Reduction::CreateLinkCommand(CreateLinkCommand::CreateConcreteConstraintStmt),
        841 => Reduction::CreateLinkCommand(CreateLinkCommand::CreateConcreteIndexStmt),
        842 => Reduction::CreateLinkCommand(CreateLinkCommand::CreateConcretePropertyStmt),
        843 => Reduction::CreateLinkCommand(CreateLinkCommand::CreateRewriteStmt),
        844 => Reduction::CreateLinkCommand(CreateLinkCommand::CreateSimpleExtending),
        845 => Reduction::CreateLinkCommand(CreateLinkCommand::SetFieldStmt),
        846 => Reduction::CreateLinkCommandsBlock(CreateLinkCommandsBlock::LBRACE_CreateLinkCommandsList_OptSemicolons_RBRACE),
        847 => Reduction::CreateLinkCommandsBlock(CreateLinkCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        848 => Reduction::CreateLinkCommandsBlock(CreateLinkCommandsBlock::LBRACE_Semicolons_CreateLinkCommandsList_OptSemicolons_RBRACE),
        849 => Reduction::CreateLinkCommandsList(CreateLinkCommandsList::CreateLinkCommand),
        850 => Reduction::CreateLinkCommandsList(CreateLinkCommandsList::CreateLinkCommandsList_Semicolons_CreateLinkCommand),
        851 => Reduction::CreateLinkSDLCommandBlock(CreateLinkSDLCommandBlock::ConcreteConstraintBlock),
        852 => Reduction::CreateLinkSDLCommandBlock(CreateLinkSDLCommandBlock::ConcreteIndexDeclarationBlock),
        853 => Reduction::CreateLinkSDLCommandBlock(CreateLinkSDLCommandBlock::ConcretePropertyBlock),
        854 => Reduction::CreateLinkSDLCommandBlock(CreateLinkSDLCommandBlock::ConcreteUnknownPointerBlock),
        855 => Reduction::CreateLinkSDLCommandBlock(CreateLinkSDLCommandBlock::RewriteDeclarationBlock),
        856 => Reduction::CreateLinkSDLCommandFull(CreateLinkSDLCommandFull::CreateLinkSDLCommandBlock),
        857 => Reduction::CreateLinkSDLCommandFull(CreateLinkSDLCommandFull::CreateLinkSDLCommandShort_SEMICOLON),
        858 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::ConcreteConstraintShort),
        859 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::ConcreteIndexDeclarationShort),
        860 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::ConcretePropertyShort),
        861 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::ConcreteUnknownPointerShort),
        862 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::CreateSimpleExtending),
        863 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::RewriteDeclarationShort),
        864 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::SetAnnotation),
        865 => Reduction::CreateLinkSDLCommandShort(CreateLinkSDLCommandShort::SetField),
        866 => Reduction::CreateLinkSDLCommandsBlock(CreateLinkSDLCommandsBlock::LBRACE_OptSemicolons_CreateLinkSDLCommandShort_RBRACE),
        867 => Reduction::CreateLinkSDLCommandsBlock(CreateLinkSDLCommandsBlock::LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandShort_RBRACE),
        868 => Reduction::CreateLinkSDLCommandsBlock(CreateLinkSDLCommandsBlock::LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_RBRACE),
        869 => Reduction::CreateLinkSDLCommandsBlock(CreateLinkSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        870 => Reduction::CreateLinkSDLCommandsList(CreateLinkSDLCommandsList::CreateLinkSDLCommandFull),
        871 => Reduction::CreateLinkSDLCommandsList(CreateLinkSDLCommandsList::CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandFull),
        872 => Reduction::CreateLinkStmt(CreateLinkStmt::CREATE_ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_OptCreateLinkCommandsBlock),
        873 => Reduction::CreateMigrationCommand(CreateMigrationCommand::NestedQLBlockStmt),
        874 => Reduction::CreateMigrationCommandsBlock(CreateMigrationCommandsBlock::LBRACE_CreateMigrationCommandsList_OptSemicolons_RBRACE),
        875 => Reduction::CreateMigrationCommandsBlock(CreateMigrationCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        876 => Reduction::CreateMigrationCommandsBlock(CreateMigrationCommandsBlock::LBRACE_Semicolons_CreateMigrationCommandsList_OptSemicolons_RBRACE),
        877 => Reduction::CreateMigrationCommandsList(CreateMigrationCommandsList::CreateMigrationCommand),
        878 => Reduction::CreateMigrationCommandsList(CreateMigrationCommandsList::CreateMigrationCommandsList_Semicolons_CreateMigrationCommand),
        879 => Reduction::CreateMigrationStmt(CreateMigrationStmt::CREATE_APPLIED_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock),
        880 => Reduction::CreateMigrationStmt(CreateMigrationStmt::CREATE_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock),
        881 => Reduction::CreateModuleStmt(CreateModuleStmt::CREATE_MODULE_ModuleName_OptIfNotExists_OptCreateCommandsBlock),
        882 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterAccessPolicyStmt),
        883 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterAnnotationValueStmt),
        884 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterConcreteConstraintStmt),
        885 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterConcreteIndexStmt),
        886 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterConcreteLinkStmt),
        887 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterConcretePropertyStmt),
        888 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::AlterTriggerStmt),
        889 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateAccessPolicyStmt),
        890 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateAnnotationValueStmt),
        891 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateConcreteConstraintStmt),
        892 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateConcreteIndexStmt),
        893 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateConcreteLinkStmt),
        894 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateConcretePropertyStmt),
        895 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::CreateTriggerStmt),
        896 => Reduction::CreateObjectTypeCommand(CreateObjectTypeCommand::SetFieldStmt),
        897 => Reduction::CreateObjectTypeCommandsBlock(CreateObjectTypeCommandsBlock::LBRACE_CreateObjectTypeCommandsList_OptSemicolons_RBRACE),
        898 => Reduction::CreateObjectTypeCommandsBlock(CreateObjectTypeCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        899 => Reduction::CreateObjectTypeCommandsBlock(CreateObjectTypeCommandsBlock::LBRACE_Semicolons_CreateObjectTypeCommandsList_OptSemicolons_RBRACE),
        900 => Reduction::CreateObjectTypeCommandsList(CreateObjectTypeCommandsList::CreateObjectTypeCommand),
        901 => Reduction::CreateObjectTypeCommandsList(CreateObjectTypeCommandsList::CreateObjectTypeCommandsList_Semicolons_CreateObjectTypeCommand),
        902 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::AccessPolicyDeclarationBlock),
        903 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::ConcreteConstraintBlock),
        904 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::ConcreteIndexDeclarationBlock),
        905 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::ConcreteLinkBlock),
        906 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::ConcretePropertyBlock),
        907 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::ConcreteUnknownPointerBlock),
        908 => Reduction::CreateObjectTypeSDLCommandBlock(CreateObjectTypeSDLCommandBlock::TriggerDeclarationBlock),
        909 => Reduction::CreateObjectTypeSDLCommandFull(CreateObjectTypeSDLCommandFull::CreateObjectTypeSDLCommandBlock),
        910 => Reduction::CreateObjectTypeSDLCommandFull(CreateObjectTypeSDLCommandFull::CreateObjectTypeSDLCommandShort_SEMICOLON),
        911 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::AccessPolicyDeclarationShort),
        912 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::ConcreteConstraintShort),
        913 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::ConcreteIndexDeclarationShort),
        914 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::ConcreteLinkShort),
        915 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::ConcretePropertyShort),
        916 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::ConcreteUnknownPointerObjectShort),
        917 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::ConcreteUnknownPointerShort),
        918 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::SetAnnotation),
        919 => Reduction::CreateObjectTypeSDLCommandShort(CreateObjectTypeSDLCommandShort::TriggerDeclarationShort),
        920 => Reduction::CreateObjectTypeSDLCommandsBlock(CreateObjectTypeSDLCommandsBlock::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE),
        921 => Reduction::CreateObjectTypeSDLCommandsBlock(CreateObjectTypeSDLCommandsBlock::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE),
        922 => Reduction::CreateObjectTypeSDLCommandsBlock(CreateObjectTypeSDLCommandsBlock::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_RBRACE),
        923 => Reduction::CreateObjectTypeSDLCommandsBlock(CreateObjectTypeSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        924 => Reduction::CreateObjectTypeSDLCommandsList(CreateObjectTypeSDLCommandsList::CreateObjectTypeSDLCommandFull),
        925 => Reduction::CreateObjectTypeSDLCommandsList(CreateObjectTypeSDLCommandsList::CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandFull),
        926 => Reduction::CreateObjectTypeStmt(CreateObjectTypeStmt::CREATE_ABSTRACT_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock),
        927 => Reduction::CreateObjectTypeStmt(CreateObjectTypeStmt::CREATE_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock),
        928 => Reduction::CreateOperatorCommand(CreateOperatorCommand::AlterAnnotationValueStmt),
        929 => Reduction::CreateOperatorCommand(CreateOperatorCommand::CreateAnnotationValueStmt),
        930 => Reduction::CreateOperatorCommand(CreateOperatorCommand::OperatorCode),
        931 => Reduction::CreateOperatorCommand(CreateOperatorCommand::SetFieldStmt),
        932 => Reduction::CreateOperatorCommandsBlock(CreateOperatorCommandsBlock::CreateOperatorCommand),
        933 => Reduction::CreateOperatorCommandsBlock(CreateOperatorCommandsBlock::LBRACE_CreateOperatorCommandsList_OptSemicolons_RBRACE),
        934 => Reduction::CreateOperatorCommandsBlock(CreateOperatorCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        935 => Reduction::CreateOperatorCommandsBlock(CreateOperatorCommandsBlock::LBRACE_Semicolons_CreateOperatorCommandsList_OptSemicolons_RBRACE),
        936 => Reduction::CreateOperatorCommandsList(CreateOperatorCommandsList::CreateOperatorCommand),
        937 => Reduction::CreateOperatorCommandsList(CreateOperatorCommandsList::CreateOperatorCommandsList_Semicolons_CreateOperatorCommand),
        938 => Reduction::CreateOperatorStmt(CreateOperatorStmt::CREATE_ABSTRACT_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_OptCreateOperatorCommandsBlock),
        939 => Reduction::CreateOperatorStmt(CreateOperatorStmt::CREATE_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateOperatorCommandsBlock),
        940 => Reduction::CreatePropertyCommand(CreatePropertyCommand::AlterAnnotationValueStmt),
        941 => Reduction::CreatePropertyCommand(CreatePropertyCommand::CreateAnnotationValueStmt),
        942 => Reduction::CreatePropertyCommand(CreatePropertyCommand::CreateSimpleExtending),
        943 => Reduction::CreatePropertyCommand(CreatePropertyCommand::SetFieldStmt),
        944 => Reduction::CreatePropertyCommand(CreatePropertyCommand::UsingStmt),
        945 => Reduction::CreatePropertyCommandsBlock(CreatePropertyCommandsBlock::LBRACE_CreatePropertyCommandsList_OptSemicolons_RBRACE),
        946 => Reduction::CreatePropertyCommandsBlock(CreatePropertyCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        947 => Reduction::CreatePropertyCommandsBlock(CreatePropertyCommandsBlock::LBRACE_Semicolons_CreatePropertyCommandsList_OptSemicolons_RBRACE),
        948 => Reduction::CreatePropertyCommandsList(CreatePropertyCommandsList::CreatePropertyCommand),
        949 => Reduction::CreatePropertyCommandsList(CreatePropertyCommandsList::CreatePropertyCommandsList_Semicolons_CreatePropertyCommand),
        950 => Reduction::CreatePropertySDLCommandFull(CreatePropertySDLCommandFull::CreatePropertySDLCommandBlock),
        951 => Reduction::CreatePropertySDLCommandFull(CreatePropertySDLCommandFull::CreatePropertySDLCommandShort_SEMICOLON),
        952 => Reduction::CreatePropertySDLCommandShort(CreatePropertySDLCommandShort::CreateSimpleExtending),
        953 => Reduction::CreatePropertySDLCommandShort(CreatePropertySDLCommandShort::SetAnnotation),
        954 => Reduction::CreatePropertySDLCommandShort(CreatePropertySDLCommandShort::SetField),
        955 => Reduction::CreatePropertySDLCommandShort(CreatePropertySDLCommandShort::Using),
        956 => Reduction::CreatePropertySDLCommandsBlock(CreatePropertySDLCommandsBlock::LBRACE_OptSemicolons_CreatePropertySDLCommandShort_RBRACE),
        957 => Reduction::CreatePropertySDLCommandsBlock(CreatePropertySDLCommandsBlock::LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandShort_RBRACE),
        958 => Reduction::CreatePropertySDLCommandsBlock(CreatePropertySDLCommandsBlock::LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_RBRACE),
        959 => Reduction::CreatePropertySDLCommandsBlock(CreatePropertySDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        960 => Reduction::CreatePropertySDLCommandsList(CreatePropertySDLCommandsList::CreatePropertySDLCommandFull),
        961 => Reduction::CreatePropertySDLCommandsList(CreatePropertySDLCommandsList::CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandFull),
        962 => Reduction::CreatePropertyStmt(CreatePropertyStmt::CREATE_ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_OptCreatePropertyCommandsBlock),
        963 => Reduction::CreatePseudoTypeCommand(CreatePseudoTypeCommand::AlterAnnotationValueStmt),
        964 => Reduction::CreatePseudoTypeCommand(CreatePseudoTypeCommand::CreateAnnotationValueStmt),
        965 => Reduction::CreatePseudoTypeCommand(CreatePseudoTypeCommand::SetFieldStmt),
        966 => Reduction::CreatePseudoTypeCommandsBlock(CreatePseudoTypeCommandsBlock::LBRACE_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE),
        967 => Reduction::CreatePseudoTypeCommandsBlock(CreatePseudoTypeCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        968 => Reduction::CreatePseudoTypeCommandsBlock(CreatePseudoTypeCommandsBlock::LBRACE_Semicolons_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE),
        969 => Reduction::CreatePseudoTypeCommandsList(CreatePseudoTypeCommandsList::CreatePseudoTypeCommand),
        970 => Reduction::CreatePseudoTypeCommandsList(CreatePseudoTypeCommandsList::CreatePseudoTypeCommandsList_Semicolons_CreatePseudoTypeCommand),
        971 => Reduction::CreatePseudoTypeStmt(CreatePseudoTypeStmt::CREATE_PSEUDO_TYPE_NodeName_OptCreatePseudoTypeCommandsBlock),
        972 => Reduction::CreateRewriteCommand(CreateRewriteCommand::CreateAnnotationValueStmt),
        973 => Reduction::CreateRewriteCommand(CreateRewriteCommand::SetFieldStmt),
        974 => Reduction::CreateRewriteCommandsBlock(CreateRewriteCommandsBlock::LBRACE_CreateRewriteCommandsList_OptSemicolons_RBRACE),
        975 => Reduction::CreateRewriteCommandsBlock(CreateRewriteCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        976 => Reduction::CreateRewriteCommandsBlock(CreateRewriteCommandsBlock::LBRACE_Semicolons_CreateRewriteCommandsList_OptSemicolons_RBRACE),
        977 => Reduction::CreateRewriteCommandsList(CreateRewriteCommandsList::CreateRewriteCommand),
        978 => Reduction::CreateRewriteCommandsList(CreateRewriteCommandsList::CreateRewriteCommandsList_Semicolons_CreateRewriteCommand),
        979 => Reduction::CreateRewriteSDLCommandFull(CreateRewriteSDLCommandFull::CreateRewriteSDLCommandBlock),
        980 => Reduction::CreateRewriteSDLCommandFull(CreateRewriteSDLCommandFull::CreateRewriteSDLCommandShort_SEMICOLON),
        981 => Reduction::CreateRewriteSDLCommandShort(CreateRewriteSDLCommandShort::SetAnnotation),
        982 => Reduction::CreateRewriteSDLCommandShort(CreateRewriteSDLCommandShort::SetField),
        983 => Reduction::CreateRewriteSDLCommandsBlock(CreateRewriteSDLCommandsBlock::LBRACE_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE),
        984 => Reduction::CreateRewriteSDLCommandsBlock(CreateRewriteSDLCommandsBlock::LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE),
        985 => Reduction::CreateRewriteSDLCommandsBlock(CreateRewriteSDLCommandsBlock::LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_RBRACE),
        986 => Reduction::CreateRewriteSDLCommandsBlock(CreateRewriteSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        987 => Reduction::CreateRewriteSDLCommandsList(CreateRewriteSDLCommandsList::CreateRewriteSDLCommandFull),
        988 => Reduction::CreateRewriteSDLCommandsList(CreateRewriteSDLCommandsList::CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandFull),
        989 => Reduction::CreateRewriteStmt(CreateRewriteStmt::CREATE_REWRITE_RewriteKindList_USING_ParenExpr_OptCreateRewriteCommandsBlock),
        990 => Reduction::CreateRoleCommand(CreateRoleCommand::SetFieldStmt),
        991 => Reduction::CreateRoleCommandsBlock(CreateRoleCommandsBlock::LBRACE_CreateRoleCommandsList_OptSemicolons_RBRACE),
        992 => Reduction::CreateRoleCommandsBlock(CreateRoleCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        993 => Reduction::CreateRoleCommandsBlock(CreateRoleCommandsBlock::LBRACE_Semicolons_CreateRoleCommandsList_OptSemicolons_RBRACE),
        994 => Reduction::CreateRoleCommandsList(CreateRoleCommandsList::CreateRoleCommand),
        995 => Reduction::CreateRoleCommandsList(CreateRoleCommandsList::CreateRoleCommandsList_Semicolons_CreateRoleCommand),
        996 => Reduction::CreateRoleStmt(CreateRoleStmt::CREATE_OptSuperuser_ROLE_ShortNodeName_OptShortExtending_OptIfNotExists_OptCreateRoleCommandsBlock),
        997 => Reduction::CreateSDLCommandFull(CreateSDLCommandFull::CreateSDLCommandBlock),
        998 => Reduction::CreateSDLCommandFull(CreateSDLCommandFull::CreateSDLCommandShort_SEMICOLON),
        999 => Reduction::CreateSDLCommandShort(CreateSDLCommandShort::SetAnnotation),
        1000 => Reduction::CreateSDLCommandShort(CreateSDLCommandShort::SetField),
        1001 => Reduction::CreateSDLCommandShort(CreateSDLCommandShort::Using),
        1002 => Reduction::CreateSDLCommandsBlock(CreateSDLCommandsBlock::LBRACE_OptSemicolons_CreateSDLCommandShort_RBRACE),
        1003 => Reduction::CreateSDLCommandsBlock(CreateSDLCommandsBlock::LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_CreateSDLCommandShort_RBRACE),
        1004 => Reduction::CreateSDLCommandsBlock(CreateSDLCommandsBlock::LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_RBRACE),
        1005 => Reduction::CreateSDLCommandsBlock(CreateSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1006 => Reduction::CreateSDLCommandsList(CreateSDLCommandsList::CreateSDLCommandFull),
        1007 => Reduction::CreateSDLCommandsList(CreateSDLCommandsList::CreateSDLCommandsList_OptSemicolons_CreateSDLCommandFull),
        1008 => Reduction::CreateScalarTypeCommand(CreateScalarTypeCommand::AlterAnnotationValueStmt),
        1009 => Reduction::CreateScalarTypeCommand(CreateScalarTypeCommand::CreateAnnotationValueStmt),
        1010 => Reduction::CreateScalarTypeCommand(CreateScalarTypeCommand::CreateConcreteConstraintStmt),
        1011 => Reduction::CreateScalarTypeCommand(CreateScalarTypeCommand::SetFieldStmt),
        1012 => Reduction::CreateScalarTypeCommandsBlock(CreateScalarTypeCommandsBlock::LBRACE_CreateScalarTypeCommandsList_OptSemicolons_RBRACE),
        1013 => Reduction::CreateScalarTypeCommandsBlock(CreateScalarTypeCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1014 => Reduction::CreateScalarTypeCommandsBlock(CreateScalarTypeCommandsBlock::LBRACE_Semicolons_CreateScalarTypeCommandsList_OptSemicolons_RBRACE),
        1015 => Reduction::CreateScalarTypeCommandsList(CreateScalarTypeCommandsList::CreateScalarTypeCommand),
        1016 => Reduction::CreateScalarTypeCommandsList(CreateScalarTypeCommandsList::CreateScalarTypeCommandsList_Semicolons_CreateScalarTypeCommand),
        1017 => Reduction::CreateScalarTypeSDLCommandBlock(CreateScalarTypeSDLCommandBlock::ConcreteConstraintBlock),
        1018 => Reduction::CreateScalarTypeSDLCommandFull(CreateScalarTypeSDLCommandFull::CreateScalarTypeSDLCommandBlock),
        1019 => Reduction::CreateScalarTypeSDLCommandFull(CreateScalarTypeSDLCommandFull::CreateScalarTypeSDLCommandShort_SEMICOLON),
        1020 => Reduction::CreateScalarTypeSDLCommandShort(CreateScalarTypeSDLCommandShort::ConcreteConstraintShort),
        1021 => Reduction::CreateScalarTypeSDLCommandShort(CreateScalarTypeSDLCommandShort::SetAnnotation),
        1022 => Reduction::CreateScalarTypeSDLCommandShort(CreateScalarTypeSDLCommandShort::SetField),
        1023 => Reduction::CreateScalarTypeSDLCommandsBlock(CreateScalarTypeSDLCommandsBlock::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE),
        1024 => Reduction::CreateScalarTypeSDLCommandsBlock(CreateScalarTypeSDLCommandsBlock::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE),
        1025 => Reduction::CreateScalarTypeSDLCommandsBlock(CreateScalarTypeSDLCommandsBlock::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_RBRACE),
        1026 => Reduction::CreateScalarTypeSDLCommandsBlock(CreateScalarTypeSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1027 => Reduction::CreateScalarTypeSDLCommandsList(CreateScalarTypeSDLCommandsList::CreateScalarTypeSDLCommandFull),
        1028 => Reduction::CreateScalarTypeSDLCommandsList(CreateScalarTypeSDLCommandsList::CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandFull),
        1029 => Reduction::CreateScalarTypeStmt(CreateScalarTypeStmt::CREATE_ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock),
        1030 => Reduction::CreateScalarTypeStmt(CreateScalarTypeStmt::CREATE_FINAL_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock),
        1031 => Reduction::CreateScalarTypeStmt(CreateScalarTypeStmt::CREATE_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock),
        1032 => Reduction::CreateSimpleExtending(CreateSimpleExtending::EXTENDING_SimpleTypeNameList),
        1033 => Reduction::CreateTriggerCommand(CreateTriggerCommand::CreateAnnotationValueStmt),
        1034 => Reduction::CreateTriggerCommand(CreateTriggerCommand::SetFieldStmt),
        1035 => Reduction::CreateTriggerCommandsBlock(CreateTriggerCommandsBlock::LBRACE_CreateTriggerCommandsList_OptSemicolons_RBRACE),
        1036 => Reduction::CreateTriggerCommandsBlock(CreateTriggerCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1037 => Reduction::CreateTriggerCommandsBlock(CreateTriggerCommandsBlock::LBRACE_Semicolons_CreateTriggerCommandsList_OptSemicolons_RBRACE),
        1038 => Reduction::CreateTriggerCommandsList(CreateTriggerCommandsList::CreateTriggerCommand),
        1039 => Reduction::CreateTriggerCommandsList(CreateTriggerCommandsList::CreateTriggerCommandsList_Semicolons_CreateTriggerCommand),
        1040 => Reduction::CreateTriggerSDLCommandFull(CreateTriggerSDLCommandFull::CreateTriggerSDLCommandBlock),
        1041 => Reduction::CreateTriggerSDLCommandFull(CreateTriggerSDLCommandFull::CreateTriggerSDLCommandShort_SEMICOLON),
        1042 => Reduction::CreateTriggerSDLCommandShort(CreateTriggerSDLCommandShort::SetAnnotation),
        1043 => Reduction::CreateTriggerSDLCommandShort(CreateTriggerSDLCommandShort::SetField),
        1044 => Reduction::CreateTriggerSDLCommandsBlock(CreateTriggerSDLCommandsBlock::LBRACE_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE),
        1045 => Reduction::CreateTriggerSDLCommandsBlock(CreateTriggerSDLCommandsBlock::LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE),
        1046 => Reduction::CreateTriggerSDLCommandsBlock(CreateTriggerSDLCommandsBlock::LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_RBRACE),
        1047 => Reduction::CreateTriggerSDLCommandsBlock(CreateTriggerSDLCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1048 => Reduction::CreateTriggerSDLCommandsList(CreateTriggerSDLCommandsList::CreateTriggerSDLCommandFull),
        1049 => Reduction::CreateTriggerSDLCommandsList(CreateTriggerSDLCommandsList::CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandFull),
        1050 => Reduction::CreateTriggerStmt(CreateTriggerStmt::CREATE_TRIGGER_UnqualifiedPointerName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_OptCreateTriggerCommandsBlock),
        1051 => Reduction::DDLStmt(DDLStmt::BranchStmt),
        1052 => Reduction::DDLStmt(DDLStmt::DatabaseStmt),
        1053 => Reduction::DDLStmt(DDLStmt::ExtensionPackageStmt),
        1054 => Reduction::DDLStmt(DDLStmt::MigrationStmt),
        1055 => Reduction::DDLStmt(DDLStmt::OptWithDDLStmt),
        1056 => Reduction::DDLStmt(DDLStmt::RoleStmt),
        1057 => Reduction::DDLWithBlock(DDLWithBlock::WithBlock),
        1058 => Reduction::DatabaseName(DatabaseName::Identifier),
        1059 => Reduction::DatabaseName(DatabaseName::ReservedKeyword),
        1060 => Reduction::DatabaseStmt(DatabaseStmt::AlterDatabaseStmt),
        1061 => Reduction::DatabaseStmt(DatabaseStmt::CreateDatabaseStmt),
        1062 => Reduction::DatabaseStmt(DatabaseStmt::DropDatabaseStmt),
        1063 => Reduction::DescribeFormat(DescribeFormat::AS_DDL),
        1064 => Reduction::DescribeFormat(DescribeFormat::AS_JSON),
        1065 => Reduction::DescribeFormat(DescribeFormat::AS_SDL),
        1066 => Reduction::DescribeFormat(DescribeFormat::AS_TEXT),
        1067 => Reduction::DescribeFormat(DescribeFormat::AS_TEXT_VERBOSE),
        1068 => Reduction::DescribeFormat(DescribeFormat::epsilon),
        1069 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_CURRENT_BRANCH_CONFIG_DescribeFormat),
        1070 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_CURRENT_DATABASE_CONFIG_DescribeFormat),
        1071 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_CURRENT_MIGRATION_DescribeFormat),
        1072 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_INSTANCE_CONFIG_DescribeFormat),
        1073 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_OBJECT_NodeName_DescribeFormat),
        1074 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_ROLES_DescribeFormat),
        1075 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_SCHEMA_DescribeFormat),
        1076 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_SYSTEM_CONFIG_DescribeFormat),
        1077 => Reduction::DescribeStmt(DescribeStmt::DESCRIBE_SchemaItem_DescribeFormat),
        1078 => Reduction::DotName(DotName::DottedIdents),
        1079 => Reduction::DottedIdents(DottedIdents::AnyIdentifier),
        1080 => Reduction::DottedIdents(DottedIdents::DottedIdents_DOT_AnyIdentifier),
        1081 => Reduction::DropAccessPolicyStmt(DropAccessPolicyStmt::DROP_ACCESS_POLICY_UnqualifiedPointerName),
        1082 => Reduction::DropAliasStmt(DropAliasStmt::DROP_ALIAS_NodeName),
        1083 => Reduction::DropAnnotationStmt(DropAnnotationStmt::DROP_ABSTRACT_ANNOTATION_NodeName),
        1084 => Reduction::DropAnnotationValueStmt(DropAnnotationValueStmt::DROP_ANNOTATION_NodeName),
        1085 => Reduction::DropBranchStmt(DropBranchStmt::DROP_BRANCH_DatabaseName_BranchOptions),
        1086 => Reduction::DropCastStmt(DropCastStmt::DROP_CAST_FROM_TypeName_TO_TypeName),
        1087 => Reduction::DropConcreteConstraintStmt(DropConcreteConstraintStmt::DROP_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr),
        1088 => Reduction::DropConcreteIndexCommand(DropConcreteIndexCommand::SetFieldStmt),
        1089 => Reduction::DropConcreteIndexCommandsBlock(DropConcreteIndexCommandsBlock::LBRACE_DropConcreteIndexCommandsList_OptSemicolons_RBRACE),
        1090 => Reduction::DropConcreteIndexCommandsBlock(DropConcreteIndexCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1091 => Reduction::DropConcreteIndexCommandsBlock(DropConcreteIndexCommandsBlock::LBRACE_Semicolons_DropConcreteIndexCommandsList_OptSemicolons_RBRACE),
        1092 => Reduction::DropConcreteIndexCommandsList(DropConcreteIndexCommandsList::DropConcreteIndexCommand),
        1093 => Reduction::DropConcreteIndexCommandsList(DropConcreteIndexCommandsList::DropConcreteIndexCommandsList_Semicolons_DropConcreteIndexCommand),
        1094 => Reduction::DropConcreteIndexStmt(DropConcreteIndexStmt::DROP_INDEX_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock),
        1095 => Reduction::DropConcreteIndexStmt(DropConcreteIndexStmt::DROP_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock),
        1096 => Reduction::DropConcreteLinkCommand(DropConcreteLinkCommand::DropConcreteConstraintStmt),
        1097 => Reduction::DropConcreteLinkCommand(DropConcreteLinkCommand::DropConcreteIndexStmt),
        1098 => Reduction::DropConcreteLinkCommand(DropConcreteLinkCommand::DropConcretePropertyStmt),
        1099 => Reduction::DropConcreteLinkCommandsBlock(DropConcreteLinkCommandsBlock::LBRACE_DropConcreteLinkCommandsList_OptSemicolons_RBRACE),
        1100 => Reduction::DropConcreteLinkCommandsBlock(DropConcreteLinkCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1101 => Reduction::DropConcreteLinkCommandsBlock(DropConcreteLinkCommandsBlock::LBRACE_Semicolons_DropConcreteLinkCommandsList_OptSemicolons_RBRACE),
        1102 => Reduction::DropConcreteLinkCommandsList(DropConcreteLinkCommandsList::DropConcreteLinkCommand),
        1103 => Reduction::DropConcreteLinkCommandsList(DropConcreteLinkCommandsList::DropConcreteLinkCommandsList_Semicolons_DropConcreteLinkCommand),
        1104 => Reduction::DropConcreteLinkStmt(DropConcreteLinkStmt::DROP_LINK_UnqualifiedPointerName_OptDropConcreteLinkCommandsBlock),
        1105 => Reduction::DropConcretePropertyStmt(DropConcretePropertyStmt::DROP_PROPERTY_UnqualifiedPointerName),
        1106 => Reduction::DropConstraintStmt(DropConstraintStmt::DROP_ABSTRACT_CONSTRAINT_NodeName),
        1107 => Reduction::DropDatabaseStmt(DropDatabaseStmt::DROP_DATABASE_DatabaseName),
        1108 => Reduction::DropExtensionPackageStmt(DropExtensionPackageStmt::DROP_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion),
        1109 => Reduction::DropExtensionStmt(DropExtensionStmt::DROP_EXTENSION_ShortNodeName_OptExtensionVersion),
        1110 => Reduction::DropFunctionStmt(DropFunctionStmt::DROP_FUNCTION_NodeName_CreateFunctionArgs),
        1111 => Reduction::DropFutureStmt(DropFutureStmt::DROP_FUTURE_ShortNodeName),
        1112 => Reduction::DropGlobalStmt(DropGlobalStmt::DROP_GLOBAL_NodeName),
        1113 => Reduction::DropIndexMatchStmt(DropIndexMatchStmt::DROP_INDEX_MATCH_FOR_TypeName_USING_NodeName),
        1114 => Reduction::DropIndexStmt(DropIndexStmt::DROP_ABSTRACT_INDEX_NodeName),
        1115 => Reduction::DropLinkCommand(DropLinkCommand::DropConcreteConstraintStmt),
        1116 => Reduction::DropLinkCommand(DropLinkCommand::DropConcreteIndexStmt),
        1117 => Reduction::DropLinkCommand(DropLinkCommand::DropConcretePropertyStmt),
        1118 => Reduction::DropLinkCommandsBlock(DropLinkCommandsBlock::LBRACE_DropLinkCommandsList_OptSemicolons_RBRACE),
        1119 => Reduction::DropLinkCommandsBlock(DropLinkCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1120 => Reduction::DropLinkCommandsBlock(DropLinkCommandsBlock::LBRACE_Semicolons_DropLinkCommandsList_OptSemicolons_RBRACE),
        1121 => Reduction::DropLinkCommandsList(DropLinkCommandsList::DropLinkCommand),
        1122 => Reduction::DropLinkCommandsList(DropLinkCommandsList::DropLinkCommandsList_Semicolons_DropLinkCommand),
        1123 => Reduction::DropLinkStmt(DropLinkStmt::DROP_ABSTRACT_LINK_PtrNodeName_OptDropLinkCommandsBlock),
        1124 => Reduction::DropMigrationStmt(DropMigrationStmt::DROP_MIGRATION_NodeName),
        1125 => Reduction::DropModuleStmt(DropModuleStmt::DROP_MODULE_ModuleName),
        1126 => Reduction::DropObjectTypeCommand(DropObjectTypeCommand::DropConcreteConstraintStmt),
        1127 => Reduction::DropObjectTypeCommand(DropObjectTypeCommand::DropConcreteIndexStmt),
        1128 => Reduction::DropObjectTypeCommand(DropObjectTypeCommand::DropConcreteLinkStmt),
        1129 => Reduction::DropObjectTypeCommand(DropObjectTypeCommand::DropConcretePropertyStmt),
        1130 => Reduction::DropObjectTypeCommandsBlock(DropObjectTypeCommandsBlock::LBRACE_DropObjectTypeCommandsList_OptSemicolons_RBRACE),
        1131 => Reduction::DropObjectTypeCommandsBlock(DropObjectTypeCommandsBlock::LBRACE_OptSemicolons_RBRACE),
        1132 => Reduction::DropObjectTypeCommandsBlock(DropObjectTypeCommandsBlock::LBRACE_Semicolons_DropObjectTypeCommandsList_OptSemicolons_RBRACE),
        1133 => Reduction::DropObjectTypeCommandsList(DropObjectTypeCommandsList::DropObjectTypeCommand),
        1134 => Reduction::DropObjectTypeCommandsList(DropObjectTypeCommandsList::DropObjectTypeCommandsList_Semicolons_DropObjectTypeCommand),
        1135 => Reduction::DropObjectTypeStmt(DropObjectTypeStmt::DROP_TYPE_NodeName_OptDropObjectTypeCommandsBlock),
        1136 => Reduction::DropOperatorStmt(DropOperatorStmt::DROP_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs),
        1137 => Reduction::DropPropertyStmt(DropPropertyStmt::DROP_ABSTRACT_PROPERTY_PtrNodeName),
        1138 => Reduction::DropRewriteStmt(DropRewriteStmt::DROP_REWRITE_RewriteKindList),
        1139 => Reduction::DropRoleStmt(DropRoleStmt::DROP_ROLE_ShortNodeName),
        1140 => Reduction::DropScalarTypeStmt(DropScalarTypeStmt::DROP_SCALAR_TYPE_NodeName),
        1141 => Reduction::DropTriggerStmt(DropTriggerStmt::DROP_TRIGGER_UnqualifiedPointerName),
        1142 => Reduction::EdgeQLBlock(EdgeQLBlock::OptSemicolons),
        1143 => Reduction::EdgeQLBlock(EdgeQLBlock::StatementBlock_OptSemicolons),
        1144 => Reduction::EdgeQLGrammar(EdgeQLGrammar::STARTBLOCK_EdgeQLBlock_EOI),
        1145 => Reduction::EdgeQLGrammar(EdgeQLGrammar::STARTEXTENSION_CreateExtensionPackageCommandsBlock_EOI),
        1146 => Reduction::EdgeQLGrammar(EdgeQLGrammar::STARTFRAGMENT_ExprStmt_EOI),
        1147 => Reduction::EdgeQLGrammar(EdgeQLGrammar::STARTFRAGMENT_Expr_EOI),
        1148 => Reduction::EdgeQLGrammar(EdgeQLGrammar::STARTMIGRATION_CreateMigrationCommandsBlock_EOI),
        1149 => Reduction::EdgeQLGrammar(EdgeQLGrammar::STARTSDLDOCUMENT_SDLDocument_EOI),
        1150 => Reduction::Expr(Expr::BaseAtomicExpr),
        1151 => Reduction::Expr(Expr::DETACHED_Expr),
        1152 => Reduction::Expr(Expr::DISTINCT_Expr),
        1153 => Reduction::Expr(Expr::EXISTS_Expr),
        1154 => Reduction::Expr(Expr::Expr_AND_Expr),
        1155 => Reduction::Expr(Expr::Expr_CIRCUMFLEX_Expr),
        1156 => Reduction::Expr(Expr::Expr_CompareOp_Expr_P_COMPARE_OP),
        1157 => Reduction::Expr(Expr::Expr_DOUBLEPLUS_Expr),
        1158 => Reduction::Expr(Expr::Expr_DOUBLEQMARK_Expr_P_DOUBLEQMARK_OP),
        1159 => Reduction::Expr(Expr::Expr_DOUBLESLASH_Expr),
        1160 => Reduction::Expr(Expr::Expr_EXCEPT_Expr),
        1161 => Reduction::Expr(Expr::Expr_IF_Expr_ELSE_Expr),
        1162 => Reduction::Expr(Expr::Expr_ILIKE_Expr),
        1163 => Reduction::Expr(Expr::Expr_INTERSECT_Expr),
        1164 => Reduction::Expr(Expr::Expr_IN_Expr),
        1165 => Reduction::Expr(Expr::Expr_IS_NOT_TypeExpr_P_IS),
        1166 => Reduction::Expr(Expr::Expr_IS_TypeExpr),
        1167 => Reduction::Expr(Expr::Expr_IndirectionEl),
        1168 => Reduction::Expr(Expr::Expr_LIKE_Expr),
        1169 => Reduction::Expr(Expr::Expr_MINUS_Expr),
        1170 => Reduction::Expr(Expr::Expr_NOT_ILIKE_Expr),
        1171 => Reduction::Expr(Expr::Expr_NOT_IN_Expr_P_IN),
        1172 => Reduction::Expr(Expr::Expr_NOT_LIKE_Expr),
        1173 => Reduction::Expr(Expr::Expr_OR_Expr),
        1174 => Reduction::Expr(Expr::Expr_PERCENT_Expr),
        1175 => Reduction::Expr(Expr::Expr_PLUS_Expr),
        1176 => Reduction::Expr(Expr::Expr_SLASH_Expr),
        1177 => Reduction::Expr(Expr::Expr_STAR_Expr),
        1178 => Reduction::Expr(Expr::Expr_Shape),
        1179 => Reduction::Expr(Expr::Expr_UNION_Expr),
        1180 => Reduction::Expr(Expr::GLOBAL_NodeName),
        1181 => Reduction::Expr(Expr::INTROSPECT_TypeExpr),
        1182 => Reduction::Expr(Expr::IfThenElseExpr),
        1183 => Reduction::Expr(Expr::LANGBRACKET_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST),
        1184 => Reduction::Expr(Expr::LANGBRACKET_OPTIONAL_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST),
        1185 => Reduction::Expr(Expr::LANGBRACKET_REQUIRED_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST),
        1186 => Reduction::Expr(Expr::MINUS_Expr_P_UMINUS),
        1187 => Reduction::Expr(Expr::NOT_Expr),
        1188 => Reduction::Expr(Expr::PLUS_Expr_P_UMINUS),
        1189 => Reduction::Expr(Expr::Path),
        1190 => Reduction::ExprList(ExprList::ExprListInner),
        1191 => Reduction::ExprList(ExprList::ExprListInner_COMMA),
        1192 => Reduction::ExprListInner(ExprListInner::Expr),
        1193 => Reduction::ExprListInner(ExprListInner::ExprListInner_COMMA_Expr),
        1194 => Reduction::ExprStmt(ExprStmt::ExprStmtCore),
        1195 => Reduction::ExprStmt(ExprStmt::WithBlock_ExprStmtCore),
        1196 => Reduction::ExprStmtCore(ExprStmtCore::InternalGroup),
        1197 => Reduction::ExprStmtCore(ExprStmtCore::SimpleDelete),
        1198 => Reduction::ExprStmtCore(ExprStmtCore::SimpleFor),
        1199 => Reduction::ExprStmtCore(ExprStmtCore::SimpleGroup),
        1200 => Reduction::ExprStmtCore(ExprStmtCore::SimpleInsert),
        1201 => Reduction::ExprStmtCore(ExprStmtCore::SimpleSelect),
        1202 => Reduction::ExprStmtCore(ExprStmtCore::SimpleUpdate),
        1203 => Reduction::Extending(Extending::EXTENDING_TypeNameList),
        1204 => Reduction::ExtendingSimple(ExtendingSimple::EXTENDING_SimpleTypeNameList),
        1205 => Reduction::ExtensionPackageStmt(ExtensionPackageStmt::CreateExtensionPackageStmt),
        1206 => Reduction::ExtensionPackageStmt(ExtensionPackageStmt::DropExtensionPackageStmt),
        1207 => Reduction::ExtensionRequirementDeclaration(ExtensionRequirementDeclaration::USING_EXTENSION_ShortNodeName_OptExtensionVersion),
        1208 => Reduction::ExtensionStmt(ExtensionStmt::CreateExtensionStmt),
        1209 => Reduction::ExtensionStmt(ExtensionStmt::DropExtensionStmt),
        1210 => Reduction::ExtensionVersion(ExtensionVersion::VERSION_BaseStringConstant),
        1211 => Reduction::FilterClause(FilterClause::FILTER_Expr),
        1212 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::FreeSimpleShapePointer_ASSIGN_Expr),
        1213 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::MULTI_FreeSimpleShapePointer_ASSIGN_Expr),
        1214 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::OPTIONAL_FreeSimpleShapePointer_ASSIGN_Expr),
        1215 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::OPTIONAL_MULTI_FreeSimpleShapePointer_ASSIGN_Expr),
        1216 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::OPTIONAL_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr),
        1217 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::REQUIRED_FreeSimpleShapePointer_ASSIGN_Expr),
        1218 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::REQUIRED_MULTI_FreeSimpleShapePointer_ASSIGN_Expr),
        1219 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::REQUIRED_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr),
        1220 => Reduction::FreeComputableShapePointer(FreeComputableShapePointer::SINGLE_FreeSimpleShapePointer_ASSIGN_Expr),
        1221 => Reduction::FreeComputableShapePointerList(FreeComputableShapePointerList::FreeComputableShapePointerListInner),
        1222 => Reduction::FreeComputableShapePointerList(FreeComputableShapePointerList::FreeComputableShapePointerListInner_COMMA),
        1223 => Reduction::FreeComputableShapePointerListInner(FreeComputableShapePointerListInner::FreeComputableShapePointer),
        1224 => Reduction::FreeComputableShapePointerListInner(FreeComputableShapePointerListInner::FreeComputableShapePointerListInner_COMMA_FreeComputableShapePointer),
        1225 => Reduction::FreeShape(FreeShape::LBRACE_FreeComputableShapePointerList_RBRACE),
        1226 => Reduction::FreeSimpleShapePointer(FreeSimpleShapePointer::FreeStepName),
        1227 => Reduction::FreeStepName(FreeStepName::DUNDERTYPE),
        1228 => Reduction::FreeStepName(FreeStepName::ShortNodeName),
        1229 => Reduction::FromFunction(FromFunction::USING_Identifier_BaseStringConstant),
        1230 => Reduction::FromFunction(FromFunction::USING_Identifier_EXPRESSION),
        1231 => Reduction::FromFunction(FromFunction::USING_Identifier_FUNCTION_BaseStringConstant),
        1232 => Reduction::FromFunction(FromFunction::USING_ParenExpr),
        1233 => Reduction::FullTypeExpr(FullTypeExpr::FullTypeExpr_AMPER_FullTypeExpr),
        1234 => Reduction::FullTypeExpr(FullTypeExpr::FullTypeExpr_PIPE_FullTypeExpr),
        1235 => Reduction::FullTypeExpr(FullTypeExpr::LPAREN_FullTypeExpr_RPAREN),
        1236 => Reduction::FullTypeExpr(FullTypeExpr::TYPEOF_Expr),
        1237 => Reduction::FullTypeExpr(FullTypeExpr::TypeName),
        1238 => Reduction::FuncApplication(FuncApplication::NodeName_LPAREN_OptFuncArgList_RPAREN),
        1239 => Reduction::FuncArgList(FuncArgList::FuncArgListInner),
        1240 => Reduction::FuncArgList(FuncArgList::FuncArgListInner_COMMA),
        1241 => Reduction::FuncArgListInner(FuncArgListInner::FuncArgListInner_COMMA_FuncCallArg),
        1242 => Reduction::FuncArgListInner(FuncArgListInner::FuncCallArg),
        1243 => Reduction::FuncCallArg(FuncCallArg::FuncCallArgExpr_OptFilterClause_OptSortClause),
        1244 => Reduction::FuncCallArgExpr(FuncCallArgExpr::AnyIdentifier_ASSIGN_Expr),
        1245 => Reduction::FuncCallArgExpr(FuncCallArgExpr::Expr),
        1246 => Reduction::FuncCallArgExpr(FuncCallArgExpr::PARAMETER_ASSIGN_Expr),
        1247 => Reduction::FuncDeclArg(FuncDeclArg::OptParameterKind_FuncDeclArgName_OptDefault),
        1248 => Reduction::FuncDeclArg(FuncDeclArg::OptParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault),
        1249 => Reduction::FuncDeclArgList(FuncDeclArgList::FuncDeclArgListInner),
        1250 => Reduction::FuncDeclArgList(FuncDeclArgList::FuncDeclArgListInner_COMMA),
        1251 => Reduction::FuncDeclArgListInner(FuncDeclArgListInner::FuncDeclArg),
        1252 => Reduction::FuncDeclArgListInner(FuncDeclArgListInner::FuncDeclArgListInner_COMMA_FuncDeclArg),
        1253 => Reduction::FuncDeclArgName(FuncDeclArgName::Identifier),
        1254 => Reduction::FuncDeclArgName(FuncDeclArgName::PARAMETER),
        1255 => Reduction::FuncDeclArgs(FuncDeclArgs::FuncDeclArgList),
        1256 => Reduction::FuncExpr(FuncExpr::FuncApplication),
        1257 => Reduction::FunctionDeclaration(FunctionDeclaration::FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSDLCommandsBlock),
        1258 => Reduction::FunctionDeclarationShort(FunctionDeclarationShort::FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSingleSDLCommandBlock),
        1259 => Reduction::FunctionType(FunctionType::FullTypeExpr),
        1260 => Reduction::FutureRequirementDeclaration(FutureRequirementDeclaration::USING_FUTURE_ShortNodeName),
        1261 => Reduction::FutureStmt(FutureStmt::CreateFutureStmt),
        1262 => Reduction::FutureStmt(FutureStmt::DropFutureStmt),
        1263 => Reduction::GlobalDeclaration(GlobalDeclaration::GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock),
        1264 => Reduction::GlobalDeclaration(GlobalDeclaration::PtrQuals_GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock),
        1265 => Reduction::GlobalDeclarationShort(GlobalDeclarationShort::GLOBAL_NodeName_ASSIGN_Expr),
        1266 => Reduction::GlobalDeclarationShort(GlobalDeclarationShort::PtrQuals_GLOBAL_NodeName_ASSIGN_Expr),
        1267 => Reduction::GlobalDeclarationShort(GlobalDeclarationShort::GLOBAL_NodeName_PtrTarget),
        1268 => Reduction::GlobalDeclarationShort(GlobalDeclarationShort::PtrQuals_GLOBAL_NodeName_PtrTarget),
        1269 => Reduction::GroupingAtom(GroupingAtom::GroupingIdent),
        1270 => Reduction::GroupingAtom(GroupingAtom::LPAREN_GroupingIdentList_RPAREN),
        1271 => Reduction::GroupingAtomList(GroupingAtomList::GroupingAtomListInner),
        1272 => Reduction::GroupingAtomList(GroupingAtomList::GroupingAtomListInner_COMMA),
        1273 => Reduction::GroupingAtomListInner(GroupingAtomListInner::GroupingAtom),
        1274 => Reduction::GroupingAtomListInner(GroupingAtomListInner::GroupingAtomListInner_COMMA_GroupingAtom),
        1275 => Reduction::GroupingElement(GroupingElement::CUBE_LPAREN_GroupingAtomList_RPAREN),
        1276 => Reduction::GroupingElement(GroupingElement::GroupingAtom),
        1277 => Reduction::GroupingElement(GroupingElement::LBRACE_GroupingElementList_RBRACE),
        1278 => Reduction::GroupingElement(GroupingElement::ROLLUP_LPAREN_GroupingAtomList_RPAREN),
        1279 => Reduction::GroupingElementList(GroupingElementList::GroupingElementListInner),
        1280 => Reduction::GroupingElementList(GroupingElementList::GroupingElementListInner_COMMA),
        1281 => Reduction::GroupingElementListInner(GroupingElementListInner::GroupingElement),
        1282 => Reduction::GroupingElementListInner(GroupingElementListInner::GroupingElementListInner_COMMA_GroupingElement),
        1283 => Reduction::GroupingIdent(GroupingIdent::AT_Identifier),
        1284 => Reduction::GroupingIdent(GroupingIdent::DOT_Identifier),
        1285 => Reduction::GroupingIdent(GroupingIdent::Identifier),
        1286 => Reduction::GroupingIdentList(GroupingIdentList::GroupingIdent),
        1287 => Reduction::GroupingIdentList(GroupingIdentList::GroupingIdentList_COMMA_GroupingIdent),
        1288 => Reduction::Identifier(Identifier::IDENT),
        1289 => Reduction::Identifier(Identifier::UnreservedKeyword),
        1290 => Reduction::IfThenElseExpr(IfThenElseExpr::IF_Expr_THEN_Expr_ELSE_Expr),
        1291 => Reduction::IndexArg(IndexArg::AnyIdentifier_ASSIGN_Expr),
        1292 => Reduction::IndexArg(IndexArg::FuncDeclArgName_OptDefault),
        1293 => Reduction::IndexArg(IndexArg::FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault),
        1294 => Reduction::IndexArg(IndexArg::ParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault),
        1295 => Reduction::IndexArgList(IndexArgList::IndexArgListInner),
        1296 => Reduction::IndexArgList(IndexArgList::IndexArgListInner_COMMA),
        1297 => Reduction::IndexArgListInner(IndexArgListInner::IndexArg),
        1298 => Reduction::IndexArgListInner(IndexArgListInner::IndexArgListInner_COMMA_IndexArg),
        1299 => Reduction::IndexDeclaration(IndexDeclaration::ABSTRACT_INDEX_NodeName_OptExtendingSimple_CreateIndexSDLCommandsBlock),
        1300 => Reduction::IndexDeclaration(IndexDeclaration::ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_CreateIndexSDLCommandsBlock),
        1301 => Reduction::IndexDeclarationShort(IndexDeclarationShort::ABSTRACT_INDEX_NodeName_OptExtendingSimple),
        1302 => Reduction::IndexDeclarationShort(IndexDeclarationShort::ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple),
        1303 => Reduction::IndexExtArgList(IndexExtArgList::LPAREN_OptIndexArgList_RPAREN),
        1304 => Reduction::IndirectionEl(IndirectionEl::LBRACKET_COLON_Expr_RBRACKET),
        1305 => Reduction::IndirectionEl(IndirectionEl::LBRACKET_Expr_COLON_Expr_RBRACKET),
        1306 => Reduction::IndirectionEl(IndirectionEl::LBRACKET_Expr_COLON_RBRACKET),
        1307 => Reduction::IndirectionEl(IndirectionEl::LBRACKET_Expr_RBRACKET),
        1308 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterAliasStmt),
        1309 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterAnnotationStmt),
        1310 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterCastStmt),
        1311 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterConstraintStmt),
        1312 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterFunctionStmt),
        1313 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterGlobalStmt),
        1314 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterIndexStmt),
        1315 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterLinkStmt),
        1316 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterModuleStmt),
        1317 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterObjectTypeStmt),
        1318 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterOperatorStmt),
        1319 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterPropertyStmt),
        1320 => Reduction::InnerDDLStmt(InnerDDLStmt::AlterScalarTypeStmt),
        1321 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateAliasStmt),
        1322 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateAnnotationStmt),
        1323 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateCastStmt),
        1324 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateConstraintStmt),
        1325 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateFunctionStmt),
        1326 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateGlobalStmt),
        1327 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateIndexMatchStmt),
        1328 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateIndexStmt),
        1329 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateLinkStmt),
        1330 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateModuleStmt),
        1331 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateObjectTypeStmt),
        1332 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateOperatorStmt),
        1333 => Reduction::InnerDDLStmt(InnerDDLStmt::CreatePropertyStmt),
        1334 => Reduction::InnerDDLStmt(InnerDDLStmt::CreatePseudoTypeStmt),
        1335 => Reduction::InnerDDLStmt(InnerDDLStmt::CreateScalarTypeStmt),
        1336 => Reduction::InnerDDLStmt(InnerDDLStmt::DropAliasStmt),
        1337 => Reduction::InnerDDLStmt(InnerDDLStmt::DropAnnotationStmt),
        1338 => Reduction::InnerDDLStmt(InnerDDLStmt::DropCastStmt),
        1339 => Reduction::InnerDDLStmt(InnerDDLStmt::DropConstraintStmt),
        1340 => Reduction::InnerDDLStmt(InnerDDLStmt::DropFunctionStmt),
        1341 => Reduction::InnerDDLStmt(InnerDDLStmt::DropGlobalStmt),
        1342 => Reduction::InnerDDLStmt(InnerDDLStmt::DropIndexMatchStmt),
        1343 => Reduction::InnerDDLStmt(InnerDDLStmt::DropIndexStmt),
        1344 => Reduction::InnerDDLStmt(InnerDDLStmt::DropLinkStmt),
        1345 => Reduction::InnerDDLStmt(InnerDDLStmt::DropModuleStmt),
        1346 => Reduction::InnerDDLStmt(InnerDDLStmt::DropObjectTypeStmt),
        1347 => Reduction::InnerDDLStmt(InnerDDLStmt::DropOperatorStmt),
        1348 => Reduction::InnerDDLStmt(InnerDDLStmt::DropPropertyStmt),
        1349 => Reduction::InnerDDLStmt(InnerDDLStmt::DropScalarTypeStmt),
        1350 => Reduction::InnerDDLStmt(InnerDDLStmt::ExtensionStmt),
        1351 => Reduction::InnerDDLStmt(InnerDDLStmt::FutureStmt),
        1352 => Reduction::InternalGroup(InternalGroup::FOR_GROUP_OptionallyAliasedExpr_UsingClause_ByClause_IN_Identifier_OptGroupingAlias_UNION_OptionallyAliasedExpr_OptFilterClause_OptSortClause),
        1353 => Reduction::LimitClause(LimitClause::LIMIT_Expr),
        1354 => Reduction::LinkDeclaration(LinkDeclaration::ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_CreateLinkSDLCommandsBlock),
        1355 => Reduction::LinkDeclarationShort(LinkDeclarationShort::ABSTRACT_LINK_PtrNodeName_OptExtendingSimple),
        1356 => Reduction::MigrationStmt(MigrationStmt::AbortMigrationStmt),
        1357 => Reduction::MigrationStmt(MigrationStmt::AlterCurrentMigrationStmt),
        1358 => Reduction::MigrationStmt(MigrationStmt::AlterMigrationStmt),
        1359 => Reduction::MigrationStmt(MigrationStmt::CommitMigrationStmt),
        1360 => Reduction::MigrationStmt(MigrationStmt::CreateMigrationStmt),
        1361 => Reduction::MigrationStmt(MigrationStmt::DropMigrationStmt),
        1362 => Reduction::MigrationStmt(MigrationStmt::PopulateMigrationStmt),
        1363 => Reduction::MigrationStmt(MigrationStmt::ResetSchemaStmt),
        1364 => Reduction::MigrationStmt(MigrationStmt::StartMigrationStmt),
        1365 => Reduction::ModuleDeclaration(ModuleDeclaration::MODULE_ModuleName_SDLCommandBlock),
        1366 => Reduction::ModuleName(ModuleName::DotName),
        1367 => Reduction::ModuleName(ModuleName::ModuleName_DOUBLECOLON_DotName),
        1368 => Reduction::NamedTuple(NamedTuple::LPAREN_NamedTupleElementList_RPAREN),
        1369 => Reduction::NamedTupleElement(NamedTupleElement::ShortNodeName_ASSIGN_Expr),
        1370 => Reduction::NamedTupleElementList(NamedTupleElementList::NamedTupleElementListInner),
        1371 => Reduction::NamedTupleElementList(NamedTupleElementList::NamedTupleElementListInner_COMMA),
        1372 => Reduction::NamedTupleElementListInner(NamedTupleElementListInner::NamedTupleElement),
        1373 => Reduction::NamedTupleElementListInner(NamedTupleElementListInner::NamedTupleElementListInner_COMMA_NamedTupleElement),
        1374 => Reduction::NestedQLBlockStmt(NestedQLBlockStmt::OptWithDDLStmt),
        1375 => Reduction::NestedQLBlockStmt(NestedQLBlockStmt::SetFieldStmt),
        1376 => Reduction::NestedQLBlockStmt(NestedQLBlockStmt::Stmt),
        1377 => Reduction::NodeName(NodeName::BaseName),
        1378 => unreachable!(),
        1379 => Reduction::NontrivialTypeExpr(NontrivialTypeExpr::LPAREN_FullTypeExpr_RPAREN),
        1380 => Reduction::NontrivialTypeExpr(NontrivialTypeExpr::TYPEOF_Expr),
        1381 => Reduction::NontrivialTypeExpr(NontrivialTypeExpr::TypeExpr_AMPER_TypeExpr),
        1382 => Reduction::NontrivialTypeExpr(NontrivialTypeExpr::TypeExpr_PIPE_TypeExpr),
        1383 => Reduction::ObjectTypeDeclaration(ObjectTypeDeclaration::ABSTRACT_TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock),
        1384 => Reduction::ObjectTypeDeclaration(ObjectTypeDeclaration::TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock),
        1385 => Reduction::ObjectTypeDeclarationShort(ObjectTypeDeclarationShort::ABSTRACT_TYPE_NodeName_OptExtendingSimple),
        1386 => Reduction::ObjectTypeDeclarationShort(ObjectTypeDeclarationShort::TYPE_NodeName_OptExtendingSimple),
        1387 => Reduction::OffsetClause(OffsetClause::OFFSET_Expr),
        1388 => Reduction::OnExpr(OnExpr::ON_ParenExpr),
        1389 => Reduction::OnSourceDeleteResetStmt(OnSourceDeleteResetStmt::RESET_ON_SOURCE_DELETE),
        1390 => Reduction::OnSourceDeleteStmt(OnSourceDeleteStmt::ON_SOURCE_DELETE_ALLOW),
        1391 => Reduction::OnSourceDeleteStmt(OnSourceDeleteStmt::ON_SOURCE_DELETE_DELETE_TARGET),
        1392 => Reduction::OnSourceDeleteStmt(OnSourceDeleteStmt::ON_SOURCE_DELETE_DELETE_TARGET_IF_ORPHAN),
        1393 => Reduction::OnTargetDeleteResetStmt(OnTargetDeleteResetStmt::RESET_ON_TARGET_DELETE),
        1394 => Reduction::OnTargetDeleteStmt(OnTargetDeleteStmt::ON_TARGET_DELETE_ALLOW),
        1395 => Reduction::OnTargetDeleteStmt(OnTargetDeleteStmt::ON_TARGET_DELETE_DEFERRED_RESTRICT),
        1396 => Reduction::OnTargetDeleteStmt(OnTargetDeleteStmt::ON_TARGET_DELETE_DELETE_SOURCE),
        1397 => Reduction::OnTargetDeleteStmt(OnTargetDeleteStmt::ON_TARGET_DELETE_RESTRICT),
        1398 => Reduction::OperatorCode(OperatorCode::USING_Identifier_BaseStringConstant),
        1399 => Reduction::OperatorCode(OperatorCode::USING_Identifier_EXPRESSION),
        1400 => Reduction::OperatorCode(OperatorCode::USING_Identifier_FUNCTION_BaseStringConstant),
        1401 => Reduction::OperatorCode(OperatorCode::USING_Identifier_OPERATOR_BaseStringConstant),
        1402 => Reduction::OperatorKind(OperatorKind::INFIX),
        1403 => Reduction::OperatorKind(OperatorKind::POSTFIX),
        1404 => Reduction::OperatorKind(OperatorKind::PREFIX),
        1405 => Reduction::OperatorKind(OperatorKind::TERNARY),
        1406 => Reduction::OptAlterUsingClause(OptAlterUsingClause::USING_ParenExpr),
        1407 => Reduction::OptAlterUsingClause(OptAlterUsingClause::epsilon),
        1408 => Reduction::OptAnySubShape(OptAnySubShape::COLON_Shape),
        1409 => Reduction::OptAnySubShape(OptAnySubShape::epsilon),
        1410 => Reduction::OptConcreteConstraintArgList(OptConcreteConstraintArgList::LPAREN_OptPosCallArgList_RPAREN),
        1411 => Reduction::OptConcreteConstraintArgList(OptConcreteConstraintArgList::epsilon),
        1412 => Reduction::OptCreateAccessPolicyCommandsBlock(OptCreateAccessPolicyCommandsBlock::CreateAccessPolicyCommandsBlock),
        1413 => Reduction::OptCreateAccessPolicyCommandsBlock(OptCreateAccessPolicyCommandsBlock::epsilon),
        1414 => Reduction::OptCreateAnnotationCommandsBlock(OptCreateAnnotationCommandsBlock::CreateAnnotationCommandsBlock),
        1415 => Reduction::OptCreateAnnotationCommandsBlock(OptCreateAnnotationCommandsBlock::epsilon),
        1416 => Reduction::OptCreateCommandsBlock(OptCreateCommandsBlock::CreateCommandsBlock),
        1417 => Reduction::OptCreateCommandsBlock(OptCreateCommandsBlock::epsilon),
        1418 => Reduction::OptCreateConcreteLinkCommandsBlock(OptCreateConcreteLinkCommandsBlock::CreateConcreteLinkCommandsBlock),
        1419 => Reduction::OptCreateConcreteLinkCommandsBlock(OptCreateConcreteLinkCommandsBlock::epsilon),
        1420 => Reduction::OptCreateConcretePropertyCommandsBlock(OptCreateConcretePropertyCommandsBlock::CreateConcretePropertyCommandsBlock),
        1421 => Reduction::OptCreateConcretePropertyCommandsBlock(OptCreateConcretePropertyCommandsBlock::epsilon),
        1422 => Reduction::OptCreateDatabaseCommandsBlock(OptCreateDatabaseCommandsBlock::CreateDatabaseCommandsBlock),
        1423 => Reduction::OptCreateDatabaseCommandsBlock(OptCreateDatabaseCommandsBlock::epsilon),
        1424 => Reduction::OptCreateExtensionCommandsBlock(OptCreateExtensionCommandsBlock::CreateExtensionCommandsBlock),
        1425 => Reduction::OptCreateExtensionCommandsBlock(OptCreateExtensionCommandsBlock::epsilon),
        1426 => Reduction::OptCreateExtensionPackageCommandsBlock(OptCreateExtensionPackageCommandsBlock::CreateExtensionPackageCommandsBlock),
        1427 => Reduction::OptCreateExtensionPackageCommandsBlock(OptCreateExtensionPackageCommandsBlock::epsilon),
        1428 => Reduction::OptCreateGlobalCommandsBlock(OptCreateGlobalCommandsBlock::CreateGlobalCommandsBlock),
        1429 => Reduction::OptCreateGlobalCommandsBlock(OptCreateGlobalCommandsBlock::epsilon),
        1430 => Reduction::OptCreateIndexCommandsBlock(OptCreateIndexCommandsBlock::CreateIndexCommandsBlock),
        1431 => Reduction::OptCreateIndexCommandsBlock(OptCreateIndexCommandsBlock::epsilon),
        1432 => Reduction::OptCreateIndexMatchCommandsBlock(OptCreateIndexMatchCommandsBlock::CreateIndexMatchCommandsBlock),
        1433 => Reduction::OptCreateIndexMatchCommandsBlock(OptCreateIndexMatchCommandsBlock::epsilon),
        1434 => Reduction::OptCreateLinkCommandsBlock(OptCreateLinkCommandsBlock::CreateLinkCommandsBlock),
        1435 => Reduction::OptCreateLinkCommandsBlock(OptCreateLinkCommandsBlock::epsilon),
        1436 => Reduction::OptCreateMigrationCommandsBlock(OptCreateMigrationCommandsBlock::CreateMigrationCommandsBlock),
        1437 => Reduction::OptCreateMigrationCommandsBlock(OptCreateMigrationCommandsBlock::epsilon),
        1438 => Reduction::OptCreateObjectTypeCommandsBlock(OptCreateObjectTypeCommandsBlock::CreateObjectTypeCommandsBlock),
        1439 => Reduction::OptCreateObjectTypeCommandsBlock(OptCreateObjectTypeCommandsBlock::epsilon),
        1440 => Reduction::OptCreateOperatorCommandsBlock(OptCreateOperatorCommandsBlock::CreateOperatorCommandsBlock),
        1441 => Reduction::OptCreateOperatorCommandsBlock(OptCreateOperatorCommandsBlock::epsilon),
        1442 => Reduction::OptCreatePropertyCommandsBlock(OptCreatePropertyCommandsBlock::CreatePropertyCommandsBlock),
        1443 => Reduction::OptCreatePropertyCommandsBlock(OptCreatePropertyCommandsBlock::epsilon),
        1444 => Reduction::OptCreatePseudoTypeCommandsBlock(OptCreatePseudoTypeCommandsBlock::CreatePseudoTypeCommandsBlock),
        1445 => Reduction::OptCreatePseudoTypeCommandsBlock(OptCreatePseudoTypeCommandsBlock::epsilon),
        1446 => Reduction::OptCreateRewriteCommandsBlock(OptCreateRewriteCommandsBlock::CreateRewriteCommandsBlock),
        1447 => Reduction::OptCreateRewriteCommandsBlock(OptCreateRewriteCommandsBlock::epsilon),
        1448 => Reduction::OptCreateRoleCommandsBlock(OptCreateRoleCommandsBlock::CreateRoleCommandsBlock),
        1449 => Reduction::OptCreateRoleCommandsBlock(OptCreateRoleCommandsBlock::epsilon),
        1450 => Reduction::OptCreateScalarTypeCommandsBlock(OptCreateScalarTypeCommandsBlock::CreateScalarTypeCommandsBlock),
        1451 => Reduction::OptCreateScalarTypeCommandsBlock(OptCreateScalarTypeCommandsBlock::epsilon),
        1452 => Reduction::OptCreateTriggerCommandsBlock(OptCreateTriggerCommandsBlock::CreateTriggerCommandsBlock),
        1453 => Reduction::OptCreateTriggerCommandsBlock(OptCreateTriggerCommandsBlock::epsilon),
        1454 => Reduction::OptDefault(OptDefault::EQUALS_Expr),
        1455 => Reduction::OptDefault(OptDefault::epsilon),
        1456 => Reduction::OptDeferred(OptDeferred::DEFERRED),
        1457 => Reduction::OptDeferred(OptDeferred::epsilon),
        1458 => Reduction::OptDelegated(OptDelegated::DELEGATED),
        1459 => Reduction::OptDelegated(OptDelegated::epsilon),
        1460 => Reduction::OptDirection(OptDirection::ASC),
        1461 => Reduction::OptDirection(OptDirection::DESC),
        1462 => Reduction::OptDirection(OptDirection::epsilon),
        1463 => Reduction::OptDropConcreteIndexCommandsBlock(OptDropConcreteIndexCommandsBlock::DropConcreteIndexCommandsBlock),
        1464 => Reduction::OptDropConcreteIndexCommandsBlock(OptDropConcreteIndexCommandsBlock::epsilon),
        1465 => Reduction::OptDropConcreteLinkCommandsBlock(OptDropConcreteLinkCommandsBlock::DropConcreteLinkCommandsBlock),
        1466 => Reduction::OptDropConcreteLinkCommandsBlock(OptDropConcreteLinkCommandsBlock::epsilon),
        1467 => Reduction::OptDropLinkCommandsBlock(OptDropLinkCommandsBlock::DropLinkCommandsBlock),
        1468 => Reduction::OptDropLinkCommandsBlock(OptDropLinkCommandsBlock::epsilon),
        1469 => Reduction::OptDropObjectTypeCommandsBlock(OptDropObjectTypeCommandsBlock::DropObjectTypeCommandsBlock),
        1470 => Reduction::OptDropObjectTypeCommandsBlock(OptDropObjectTypeCommandsBlock::epsilon),
        1471 => Reduction::OptExceptExpr(OptExceptExpr::EXCEPT_ParenExpr),
        1472 => Reduction::OptExceptExpr(OptExceptExpr::epsilon),
        1473 => Reduction::OptExprList(OptExprList::ExprList),
        1474 => Reduction::OptExprList(OptExprList::epsilon),
        1475 => Reduction::OptExtending(OptExtending::Extending),
        1476 => Reduction::OptExtending(OptExtending::epsilon),
        1477 => Reduction::OptExtendingSimple(OptExtendingSimple::ExtendingSimple),
        1478 => Reduction::OptExtendingSimple(OptExtendingSimple::epsilon),
        1479 => Reduction::OptExtensionVersion(OptExtensionVersion::ExtensionVersion),
        1480 => Reduction::OptExtensionVersion(OptExtensionVersion::epsilon),
        1481 => Reduction::OptFilterClause(OptFilterClause::FilterClause),
        1482 => Reduction::OptFilterClause(OptFilterClause::epsilon),
        1483 => Reduction::OptFuncArgList(OptFuncArgList::FuncArgList),
        1484 => Reduction::OptFuncArgList(OptFuncArgList::epsilon),
        1485 => Reduction::OptGroupingAlias(OptGroupingAlias::COMMA_Identifier),
        1486 => Reduction::OptGroupingAlias(OptGroupingAlias::epsilon),
        1487 => Reduction::OptIfNotExists(OptIfNotExists::IF_NOT_EXISTS),
        1488 => Reduction::OptIfNotExists(OptIfNotExists::epsilon),
        1489 => Reduction::OptIndexArgList(OptIndexArgList::IndexArgList),
        1490 => Reduction::OptIndexArgList(OptIndexArgList::epsilon),
        1491 => Reduction::OptIndexExtArgList(OptIndexExtArgList::IndexExtArgList),
        1492 => Reduction::OptIndexExtArgList(OptIndexExtArgList::epsilon),
        1493 => Reduction::OptMigrationNameParentName(OptMigrationNameParentName::ShortNodeName),
        1494 => Reduction::OptMigrationNameParentName(OptMigrationNameParentName::ShortNodeName_ONTO_ShortNodeName),
        1495 => Reduction::OptMigrationNameParentName(OptMigrationNameParentName::epsilon),
        1496 => Reduction::OptNonesOrder(OptNonesOrder::EMPTY_FIRST),
        1497 => Reduction::OptNonesOrder(OptNonesOrder::EMPTY_LAST),
        1498 => Reduction::OptNonesOrder(OptNonesOrder::epsilon),
        1499 => Reduction::OptOnExpr(OptOnExpr::OnExpr),
        1500 => Reduction::OptOnExpr(OptOnExpr::epsilon),
        1501 => Reduction::OptParameterKind(OptParameterKind::ParameterKind),
        1502 => Reduction::OptParameterKind(OptParameterKind::epsilon),
        1503 => Reduction::OptPosCallArgList(OptPosCallArgList::PosCallArgList),
        1504 => Reduction::OptPosCallArgList(OptPosCallArgList::epsilon),
        1505 => Reduction::OptPosition(OptPosition::AFTER_NodeName),
        1506 => Reduction::OptPosition(OptPosition::BEFORE_NodeName),
        1507 => Reduction::OptPosition(OptPosition::FIRST),
        1508 => Reduction::OptPosition(OptPosition::LAST),
        1509 => Reduction::OptPosition(OptPosition::epsilon),
        1510 => Reduction::OptPtrQuals(OptPtrQuals::PtrQuals),
        1511 => Reduction::OptPtrQuals(OptPtrQuals::epsilon),
        1512 => Reduction::OptPtrTarget(OptPtrTarget::PtrTarget),
        1513 => Reduction::OptPtrTarget(OptPtrTarget::epsilon),
        1514 => Reduction::OptSelectLimit(OptSelectLimit::SelectLimit),
        1515 => Reduction::OptSelectLimit(OptSelectLimit::epsilon),
        1516 => Reduction::OptSemicolons(OptSemicolons::Semicolons),
        1517 => Reduction::OptSemicolons(OptSemicolons::epsilon),
        1518 => Reduction::OptShortExtending(OptShortExtending::ShortExtending),
        1519 => Reduction::OptShortExtending(OptShortExtending::epsilon),
        1520 => Reduction::OptSortClause(OptSortClause::SortClause),
        1521 => Reduction::OptSortClause(OptSortClause::epsilon),
        1522 => Reduction::OptSuperuser(OptSuperuser::SUPERUSER),
        1523 => Reduction::OptSuperuser(OptSuperuser::epsilon),
        1524 => Reduction::OptTransactionModeList(OptTransactionModeList::TransactionModeList),
        1525 => Reduction::OptTransactionModeList(OptTransactionModeList::epsilon),
        1526 => Reduction::OptTypeIntersection(OptTypeIntersection::TypeIntersection),
        1527 => Reduction::OptTypeIntersection(OptTypeIntersection::epsilon),
        1528 => Reduction::OptTypeQualifier(OptTypeQualifier::OPTIONAL),
        1529 => Reduction::OptTypeQualifier(OptTypeQualifier::SET_OF),
        1530 => Reduction::OptTypeQualifier(OptTypeQualifier::epsilon),
        1531 => Reduction::OptUnlessConflictClause(OptUnlessConflictClause::UnlessConflictCause),
        1532 => Reduction::OptUnlessConflictClause(OptUnlessConflictClause::epsilon),
        1533 => Reduction::OptUsingBlock(OptUsingBlock::USING_ParenExpr),
        1534 => Reduction::OptUsingBlock(OptUsingBlock::epsilon),
        1535 => Reduction::OptUsingClause(OptUsingClause::UsingClause),
        1536 => Reduction::OptUsingClause(OptUsingClause::epsilon),
        1537 => Reduction::OptWhenBlock(OptWhenBlock::WHEN_ParenExpr),
        1538 => Reduction::OptWhenBlock(OptWhenBlock::epsilon),
        1539 => Reduction::OptWithDDLStmt(OptWithDDLStmt::DDLWithBlock_WithDDLStmt),
        1540 => Reduction::OptWithDDLStmt(OptWithDDLStmt::WithDDLStmt),
        1541 => Reduction::OptionalOptional(OptionalOptional::OPTIONAL),
        1542 => Reduction::OptionalOptional(OptionalOptional::epsilon),
        1543 => Reduction::OptionallyAliasedExpr(OptionallyAliasedExpr::AliasedExpr),
        1544 => Reduction::OptionallyAliasedExpr(OptionallyAliasedExpr::Expr),
        1545 => Reduction::OrderbyExpr(OrderbyExpr::Expr_OptDirection_OptNonesOrder),
        1546 => Reduction::OrderbyList(OrderbyList::OrderbyExpr),
        1547 => Reduction::OrderbyList(OrderbyList::OrderbyList_THEN_OrderbyExpr),
        1548 => Reduction::ParameterKind(ParameterKind::NAMEDONLY),
        1549 => Reduction::ParameterKind(ParameterKind::VARIADIC),
        1550 => Reduction::ParenExpr(ParenExpr::LPAREN_ExprStmt_RPAREN),
        1551 => Reduction::ParenExpr(ParenExpr::LPAREN_Expr_RPAREN),
        1552 => Reduction::ParenTypeExpr(ParenTypeExpr::LPAREN_FullTypeExpr_RPAREN),
        1553 => Reduction::PartialReservedKeyword(PartialReservedKeyword::EXCEPT),
        1554 => Reduction::PartialReservedKeyword(PartialReservedKeyword::INTERSECT),
        1555 => Reduction::PartialReservedKeyword(PartialReservedKeyword::UNION),
        1556 => Reduction::Path(Path::Expr_PathStep_P_DOT),
        1557 => Reduction::PathNodeName(PathNodeName::PtrIdentifier),
        1558 => Reduction::PathStep(PathStep::AT_PathNodeName),
        1559 => Reduction::PathStep(PathStep::DOTBW_PathStepName),
        1560 => Reduction::PathStep(PathStep::DOT_ICONST),
        1561 => Reduction::PathStep(PathStep::DOT_PathStepName),
        1562 => Reduction::PathStep(PathStep::TypeIntersection),
        1563 => Reduction::PathStepName(PathStepName::DUNDERTYPE),
        1564 => Reduction::PathStepName(PathStepName::PathNodeName),
        1565 => Reduction::PointerName(PointerName::DUNDERTYPE),
        1566 => Reduction::PointerName(PointerName::PtrNodeName),
        1567 => Reduction::PopulateMigrationStmt(PopulateMigrationStmt::POPULATE_MIGRATION),
        1568 => Reduction::PosCallArg(PosCallArg::Expr_OptFilterClause_OptSortClause),
        1569 => Reduction::PosCallArgList(PosCallArgList::PosCallArg),
        1570 => Reduction::PosCallArgList(PosCallArgList::PosCallArgList_COMMA_PosCallArg),
        1571 => Reduction::PropertyDeclaration(PropertyDeclaration::ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_CreatePropertySDLCommandsBlock),
        1572 => Reduction::PropertyDeclarationShort(PropertyDeclarationShort::ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple),
        1573 => Reduction::PtrIdentifier(PtrIdentifier::Identifier),
        1574 => Reduction::PtrIdentifier(PtrIdentifier::PartialReservedKeyword),
        1575 => Reduction::PtrName(PtrName::PtrIdentifier),
        1576 => Reduction::PtrName(PtrName::QualifiedName),
        1577 => Reduction::PtrNodeName(PtrNodeName::PtrName),
        1578 => Reduction::PtrQualifiedNodeName(PtrQualifiedNodeName::QualifiedName),
        1579 => Reduction::PtrQuals(PtrQuals::MULTI),
        1580 => Reduction::PtrQuals(PtrQuals::OPTIONAL),
        1581 => Reduction::PtrQuals(PtrQuals::OPTIONAL_MULTI),
        1582 => Reduction::PtrQuals(PtrQuals::OPTIONAL_SINGLE),
        1583 => Reduction::PtrQuals(PtrQuals::REQUIRED),
        1584 => Reduction::PtrQuals(PtrQuals::REQUIRED_MULTI),
        1585 => Reduction::PtrQuals(PtrQuals::REQUIRED_SINGLE),
        1586 => Reduction::PtrQuals(PtrQuals::SINGLE),
        1587 => Reduction::PtrTarget(PtrTarget::ARROW_FullTypeExpr),
        1588 => Reduction::PtrTarget(PtrTarget::COLON_FullTypeExpr),
        1589 => Reduction::QualifiedName(QualifiedName::DUNDERSTD_DOUBLECOLON_ColonedIdents),
        1590 => Reduction::QualifiedName(QualifiedName::Identifier_DOUBLECOLON_ColonedIdents),
        1591 => Reduction::RenameStmt(RenameStmt::RENAME_TO_NodeName),
        1592 => Reduction::ReservedKeyword(ReservedKeyword::ADMINISTER),
        1593 => Reduction::ReservedKeyword(ReservedKeyword::ALTER),
        1594 => Reduction::ReservedKeyword(ReservedKeyword::ANALYZE),
        1595 => Reduction::ReservedKeyword(ReservedKeyword::AND),
        1596 => Reduction::ReservedKeyword(ReservedKeyword::ANYARRAY),
        1597 => Reduction::ReservedKeyword(ReservedKeyword::ANYOBJECT),
        1598 => Reduction::ReservedKeyword(ReservedKeyword::ANYTUPLE),
        1599 => Reduction::ReservedKeyword(ReservedKeyword::ANYTYPE),
        1600 => Reduction::ReservedKeyword(ReservedKeyword::BEGIN),
        1601 => Reduction::ReservedKeyword(ReservedKeyword::BY),
        1602 => Reduction::ReservedKeyword(ReservedKeyword::CASE),
        1603 => Reduction::ReservedKeyword(ReservedKeyword::CHECK),
        1604 => Reduction::ReservedKeyword(ReservedKeyword::COMMIT),
        1605 => Reduction::ReservedKeyword(ReservedKeyword::CONFIGURE),
        1606 => Reduction::ReservedKeyword(ReservedKeyword::CREATE),
        1607 => Reduction::ReservedKeyword(ReservedKeyword::DEALLOCATE),
        1608 => Reduction::ReservedKeyword(ReservedKeyword::DELETE),
        1609 => Reduction::ReservedKeyword(ReservedKeyword::DESCRIBE),
        1610 => Reduction::ReservedKeyword(ReservedKeyword::DETACHED),
        1611 => Reduction::ReservedKeyword(ReservedKeyword::DISCARD),
        1612 => Reduction::ReservedKeyword(ReservedKeyword::DISTINCT),
        1613 => Reduction::ReservedKeyword(ReservedKeyword::DO),
        1614 => Reduction::ReservedKeyword(ReservedKeyword::DROP),
        1615 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERDEFAULT),
        1616 => Reduction::ReservedKeyword(ReservedKeyword::DUNDEREDGEDBSYS),
        1617 => Reduction::ReservedKeyword(ReservedKeyword::DUNDEREDGEDBTPL),
        1618 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERNEW),
        1619 => Reduction::ReservedKeyword(ReservedKeyword::DUNDEROLD),
        1620 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERSOURCE),
        1621 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERSPECIFIED),
        1622 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERSTD),
        1623 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERSUBJECT),
        1624 => Reduction::ReservedKeyword(ReservedKeyword::DUNDERTYPE),
        1625 => Reduction::ReservedKeyword(ReservedKeyword::ELSE),
        1626 => Reduction::ReservedKeyword(ReservedKeyword::END),
        1627 => Reduction::ReservedKeyword(ReservedKeyword::EXISTS),
        1628 => Reduction::ReservedKeyword(ReservedKeyword::EXPLAIN),
        1629 => Reduction::ReservedKeyword(ReservedKeyword::EXTENDING),
        1630 => Reduction::ReservedKeyword(ReservedKeyword::FALSE),
        1631 => Reduction::ReservedKeyword(ReservedKeyword::FETCH),
        1632 => Reduction::ReservedKeyword(ReservedKeyword::FILTER),
        1633 => Reduction::ReservedKeyword(ReservedKeyword::FOR),
        1634 => Reduction::ReservedKeyword(ReservedKeyword::GET),
        1635 => Reduction::ReservedKeyword(ReservedKeyword::GLOBAL),
        1636 => Reduction::ReservedKeyword(ReservedKeyword::GRANT),
        1637 => Reduction::ReservedKeyword(ReservedKeyword::GROUP),
        1638 => Reduction::ReservedKeyword(ReservedKeyword::IF),
        1639 => Reduction::ReservedKeyword(ReservedKeyword::ILIKE),
        1640 => Reduction::ReservedKeyword(ReservedKeyword::IMPORT),
        1641 => Reduction::ReservedKeyword(ReservedKeyword::IN),
        1642 => Reduction::ReservedKeyword(ReservedKeyword::INSERT),
        1643 => Reduction::ReservedKeyword(ReservedKeyword::INTROSPECT),
        1644 => Reduction::ReservedKeyword(ReservedKeyword::IS),
        1645 => Reduction::ReservedKeyword(ReservedKeyword::LIKE),
        1646 => Reduction::ReservedKeyword(ReservedKeyword::LIMIT),
        1647 => Reduction::ReservedKeyword(ReservedKeyword::LISTEN),
        1648 => Reduction::ReservedKeyword(ReservedKeyword::LOAD),
        1649 => Reduction::ReservedKeyword(ReservedKeyword::LOCK),
        1650 => Reduction::ReservedKeyword(ReservedKeyword::MATCH),
        1651 => Reduction::ReservedKeyword(ReservedKeyword::MODULE),
        1652 => Reduction::ReservedKeyword(ReservedKeyword::MOVE),
        1653 => Reduction::ReservedKeyword(ReservedKeyword::NEVER),
        1654 => Reduction::ReservedKeyword(ReservedKeyword::NOT),
        1655 => Reduction::ReservedKeyword(ReservedKeyword::NOTIFY),
        1656 => Reduction::ReservedKeyword(ReservedKeyword::OFFSET),
        1657 => Reduction::ReservedKeyword(ReservedKeyword::ON),
        1658 => Reduction::ReservedKeyword(ReservedKeyword::OPTIONAL),
        1659 => Reduction::ReservedKeyword(ReservedKeyword::OR),
        1660 => Reduction::ReservedKeyword(ReservedKeyword::OVER),
        1661 => Reduction::ReservedKeyword(ReservedKeyword::PARTITION),
        1662 => Reduction::ReservedKeyword(ReservedKeyword::PREPARE),
        1663 => Reduction::ReservedKeyword(ReservedKeyword::RAISE),
        1664 => Reduction::ReservedKeyword(ReservedKeyword::REFRESH),
        1665 => Reduction::ReservedKeyword(ReservedKeyword::REVOKE),
        1666 => Reduction::ReservedKeyword(ReservedKeyword::ROLLBACK),
        1667 => Reduction::ReservedKeyword(ReservedKeyword::SELECT),
        1668 => Reduction::ReservedKeyword(ReservedKeyword::SET),
        1669 => Reduction::ReservedKeyword(ReservedKeyword::SINGLE),
        1670 => Reduction::ReservedKeyword(ReservedKeyword::START),
        1671 => Reduction::ReservedKeyword(ReservedKeyword::TRUE),
        1672 => Reduction::ReservedKeyword(ReservedKeyword::TYPEOF),
        1673 => Reduction::ReservedKeyword(ReservedKeyword::UPDATE),
        1674 => Reduction::ReservedKeyword(ReservedKeyword::VARIADIC),
        1675 => Reduction::ReservedKeyword(ReservedKeyword::WHEN),
        1676 => Reduction::ReservedKeyword(ReservedKeyword::WINDOW),
        1677 => Reduction::ReservedKeyword(ReservedKeyword::WITH),
        1678 => Reduction::ResetFieldStmt(ResetFieldStmt::RESET_DEFAULT),
        1679 => Reduction::ResetFieldStmt(ResetFieldStmt::RESET_IDENT),
        1680 => Reduction::ResetSchemaStmt(ResetSchemaStmt::RESET_SCHEMA_TO_NodeName),
        1681 => Reduction::ResetStmt(ResetStmt::RESET_ALIAS_Identifier),
        1682 => Reduction::ResetStmt(ResetStmt::RESET_ALIAS_STAR),
        1683 => Reduction::ResetStmt(ResetStmt::RESET_MODULE),
        1684 => Reduction::RewriteDeclarationBlock(RewriteDeclarationBlock::REWRITE_RewriteKindList_USING_ParenExpr_CreateRewriteSDLCommandsBlock),
        1685 => Reduction::RewriteDeclarationShort(RewriteDeclarationShort::REWRITE_RewriteKindList_USING_ParenExpr),
        1686 => Reduction::RewriteKind(RewriteKind::INSERT),
        1687 => Reduction::RewriteKind(RewriteKind::UPDATE),
        1688 => Reduction::RewriteKindList(RewriteKindList::RewriteKind),
        1689 => Reduction::RewriteKindList(RewriteKindList::RewriteKindList_COMMA_RewriteKind),
        1690 => Reduction::RoleStmt(RoleStmt::AlterRoleStmt),
        1691 => Reduction::RoleStmt(RoleStmt::CreateRoleStmt),
        1692 => Reduction::RoleStmt(RoleStmt::DropRoleStmt),
        1693 => Reduction::SDLBlockStatement(SDLBlockStatement::AliasDeclaration),
        1694 => Reduction::SDLBlockStatement(SDLBlockStatement::AnnotationDeclaration),
        1695 => Reduction::SDLBlockStatement(SDLBlockStatement::ConstraintDeclaration),
        1696 => Reduction::SDLBlockStatement(SDLBlockStatement::FunctionDeclaration),
        1697 => Reduction::SDLBlockStatement(SDLBlockStatement::GlobalDeclaration),
        1698 => Reduction::SDLBlockStatement(SDLBlockStatement::IndexDeclaration),
        1699 => Reduction::SDLBlockStatement(SDLBlockStatement::LinkDeclaration),
        1700 => Reduction::SDLBlockStatement(SDLBlockStatement::ModuleDeclaration),
        1701 => Reduction::SDLBlockStatement(SDLBlockStatement::ObjectTypeDeclaration),
        1702 => Reduction::SDLBlockStatement(SDLBlockStatement::PropertyDeclaration),
        1703 => Reduction::SDLBlockStatement(SDLBlockStatement::ScalarTypeDeclaration),
        1704 => Reduction::SDLCommandBlock(SDLCommandBlock::LBRACE_OptSemicolons_RBRACE),
        1705 => Reduction::SDLCommandBlock(SDLCommandBlock::LBRACE_OptSemicolons_SDLStatements_RBRACE),
        1706 => Reduction::SDLCommandBlock(SDLCommandBlock::LBRACE_OptSemicolons_SDLShortStatement_RBRACE),
        1707 => Reduction::SDLCommandBlock(SDLCommandBlock::LBRACE_OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement_RBRACE),
        1708 => Reduction::SDLCommandBlock(SDLCommandBlock::LBRACE_OptSemicolons_SDLStatements_Semicolons_RBRACE),
        1709 => Reduction::SDLDocument(SDLDocument::OptSemicolons),
        1710 => Reduction::SDLDocument(SDLDocument::OptSemicolons_SDLStatements),
        1711 => Reduction::SDLDocument(SDLDocument::OptSemicolons_SDLStatements_Semicolons),
        1712 => Reduction::SDLDocument(SDLDocument::OptSemicolons_SDLShortStatement),
        1713 => Reduction::SDLDocument(SDLDocument::OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement),
        1714 => Reduction::SDLShortStatement(SDLShortStatement::AliasDeclarationShort),
        1715 => Reduction::SDLShortStatement(SDLShortStatement::AnnotationDeclarationShort),
        1716 => Reduction::SDLShortStatement(SDLShortStatement::ConstraintDeclarationShort),
        1717 => Reduction::SDLShortStatement(SDLShortStatement::ExtensionRequirementDeclaration),
        1718 => Reduction::SDLShortStatement(SDLShortStatement::FunctionDeclarationShort),
        1719 => Reduction::SDLShortStatement(SDLShortStatement::FutureRequirementDeclaration),
        1720 => Reduction::SDLShortStatement(SDLShortStatement::GlobalDeclarationShort),
        1721 => Reduction::SDLShortStatement(SDLShortStatement::IndexDeclarationShort),
        1722 => Reduction::SDLShortStatement(SDLShortStatement::LinkDeclarationShort),
        1723 => Reduction::SDLShortStatement(SDLShortStatement::ObjectTypeDeclarationShort),
        1724 => Reduction::SDLShortStatement(SDLShortStatement::PropertyDeclarationShort),
        1725 => Reduction::SDLShortStatement(SDLShortStatement::ScalarTypeDeclarationShort),
        1726 => Reduction::SDLStatement(SDLStatement::SDLBlockStatement),
        1727 => Reduction::SDLStatement(SDLStatement::SDLShortStatement_SEMICOLON),
        1728 => Reduction::SDLStatements(SDLStatements::SDLStatement),
        1729 => Reduction::SDLStatements(SDLStatements::SDLStatements_OptSemicolons_SDLStatement),
        1730 => Reduction::ScalarTypeDeclaration(ScalarTypeDeclaration::ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock),
        1731 => Reduction::ScalarTypeDeclaration(ScalarTypeDeclaration::SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock),
        1732 => Reduction::ScalarTypeDeclarationShort(ScalarTypeDeclarationShort::ABSTRACT_SCALAR_TYPE_NodeName_OptExtending),
        1733 => Reduction::ScalarTypeDeclarationShort(ScalarTypeDeclarationShort::SCALAR_TYPE_NodeName_OptExtending),
        1734 => Reduction::SchemaItem(SchemaItem::SchemaObjectClass_NodeName),
        1735 => Reduction::SchemaObjectClass(SchemaObjectClass::ALIAS),
        1736 => Reduction::SchemaObjectClass(SchemaObjectClass::ANNOTATION),
        1737 => Reduction::SchemaObjectClass(SchemaObjectClass::CAST),
        1738 => Reduction::SchemaObjectClass(SchemaObjectClass::CONSTRAINT),
        1739 => Reduction::SchemaObjectClass(SchemaObjectClass::FUNCTION),
        1740 => Reduction::SchemaObjectClass(SchemaObjectClass::LINK),
        1741 => Reduction::SchemaObjectClass(SchemaObjectClass::MODULE),
        1742 => Reduction::SchemaObjectClass(SchemaObjectClass::OPERATOR),
        1743 => Reduction::SchemaObjectClass(SchemaObjectClass::PROPERTY),
        1744 => Reduction::SchemaObjectClass(SchemaObjectClass::SCALAR_TYPE),
        1745 => Reduction::SchemaObjectClass(SchemaObjectClass::TYPE),
        1746 => Reduction::SelectLimit(SelectLimit::LimitClause),
        1747 => Reduction::SelectLimit(SelectLimit::OffsetClause),
        1748 => Reduction::SelectLimit(SelectLimit::OffsetClause_LimitClause),
        1749 => Reduction::Semicolons(Semicolons::SEMICOLON),
        1750 => Reduction::Semicolons(Semicolons::Semicolons_SEMICOLON),
        1751 => Reduction::SessionStmt(SessionStmt::ResetStmt),
        1752 => Reduction::SessionStmt(SessionStmt::SetStmt),
        1753 => Reduction::Set(Set::LBRACE_OptExprList_RBRACE),
        1754 => Reduction::SetAnnotation(SetAnnotation::ANNOTATION_NodeName_ASSIGN_Expr),
        1755 => Reduction::SetCardinalityStmt(SetCardinalityStmt::RESET_CARDINALITY_OptAlterUsingClause),
        1756 => Reduction::SetCardinalityStmt(SetCardinalityStmt::SET_MULTI),
        1757 => Reduction::SetCardinalityStmt(SetCardinalityStmt::SET_SINGLE_OptAlterUsingClause),
        1758 => Reduction::SetDelegatedStmt(SetDelegatedStmt::RESET_DELEGATED),
        1759 => Reduction::SetDelegatedStmt(SetDelegatedStmt::SET_DELEGATED),
        1760 => Reduction::SetDelegatedStmt(SetDelegatedStmt::SET_NOT_DELEGATED),
        1761 => Reduction::SetField(SetField::Identifier_ASSIGN_Expr),
        1762 => Reduction::SetFieldStmt(SetFieldStmt::SET_Identifier_ASSIGN_Expr),
        1763 => Reduction::SetGlobalTypeStmt(SetGlobalTypeStmt::RESET_TYPE),
        1764 => Reduction::SetGlobalTypeStmt(SetGlobalTypeStmt::SETTYPE_FullTypeExpr_OptAlterUsingClause),
        1765 => Reduction::SetGlobalTypeStmt(SetGlobalTypeStmt::SETTYPE_FullTypeExpr_RESET_TO_DEFAULT),
        1766 => Reduction::SetPointerTypeStmt(SetPointerTypeStmt::RESET_TYPE),
        1767 => Reduction::SetPointerTypeStmt(SetPointerTypeStmt::SETTYPE_FullTypeExpr_OptAlterUsingClause),
        1768 => Reduction::SetRequiredInCreateStmt(SetRequiredInCreateStmt::SET_REQUIRED_OptAlterUsingClause),
        1769 => Reduction::SetRequiredStmt(SetRequiredStmt::DROP_REQUIRED),
        1770 => Reduction::SetRequiredStmt(SetRequiredStmt::RESET_OPTIONALITY),
        1771 => Reduction::SetRequiredStmt(SetRequiredStmt::SET_OPTIONAL),
        1772 => Reduction::SetRequiredStmt(SetRequiredStmt::SET_REQUIRED_OptAlterUsingClause),
        1773 => Reduction::SetStmt(SetStmt::SET_ALIAS_Identifier_AS_MODULE_ModuleName),
        1774 => Reduction::SetStmt(SetStmt::SET_MODULE_ModuleName),
        1775 => Reduction::Shape(Shape::LBRACE_RBRACE),
        1776 => Reduction::Shape(Shape::LBRACE_ShapeElementList_RBRACE),
        1777 => Reduction::ShapeElement(ShapeElement::ComputableShapePointer),
        1778 => Reduction::ShapeElement(ShapeElement::ShapePointer_OptAnySubShape_OptFilterClause_OptSortClause_OptSelectLimit),
        1779 => Reduction::ShapeElementList(ShapeElementList::ShapeElementListInner),
        1780 => Reduction::ShapeElementList(ShapeElementList::ShapeElementListInner_COMMA),
        1781 => Reduction::ShapeElementListInner(ShapeElementListInner::ShapeElement),
        1782 => Reduction::ShapeElementListInner(ShapeElementListInner::ShapeElementListInner_COMMA_ShapeElement),
        1783 => Reduction::ShapePath(ShapePath::AT_PathNodeName),
        1784 => Reduction::ShapePath(ShapePath::PathStepName_OptTypeIntersection),
        1785 => Reduction::ShapePath(ShapePath::Splat),
        1786 => Reduction::ShapePath(ShapePath::TypeIntersection_DOT_PathStepName_OptTypeIntersection),
        1787 => Reduction::ShapePointer(ShapePointer::ShapePath),
        1788 => Reduction::ShortExtending(ShortExtending::EXTENDING_ShortNodeNameList),
        1789 => Reduction::ShortNodeName(ShortNodeName::Identifier),
        1790 => Reduction::ShortNodeNameList(ShortNodeNameList::ShortNodeName),
        1791 => Reduction::ShortNodeNameList(ShortNodeNameList::ShortNodeNameList_COMMA_ShortNodeName),
        1792 => Reduction::SimpleDelete(SimpleDelete::DELETE_Expr_OptFilterClause_OptSortClause_OptSelectLimit),
        1793 => Reduction::SimpleFor(SimpleFor::FOR_OptionalOptional_Identifier_IN_AtomicExpr_UNION_Expr),
        1794 => Reduction::SimpleFor(SimpleFor::FOR_OptionalOptional_Identifier_IN_AtomicExpr_ExprStmt),
        1795 => Reduction::SimpleGroup(SimpleGroup::GROUP_OptionallyAliasedExpr_OptUsingClause_ByClause),
        1796 => Reduction::SimpleInsert(SimpleInsert::INSERT_Expr_OptUnlessConflictClause),
        1797 => Reduction::SimpleSelect(SimpleSelect::SELECT_OptionallyAliasedExpr_OptFilterClause_OptSortClause_OptSelectLimit),
        1798 => Reduction::SimpleShapePath(SimpleShapePath::AT_PathNodeName),
        1799 => Reduction::SimpleShapePath(SimpleShapePath::PathStepName),
        1800 => Reduction::SimpleShapePointer(SimpleShapePointer::SimpleShapePath),
        1801 => Reduction::SimpleTypeName(SimpleTypeName::ANYOBJECT),
        1802 => Reduction::SimpleTypeName(SimpleTypeName::ANYTUPLE),
        1803 => Reduction::SimpleTypeName(SimpleTypeName::ANYTYPE),
        1804 => Reduction::SimpleTypeName(SimpleTypeName::PtrNodeName),
        1805 => Reduction::SimpleTypeNameList(SimpleTypeNameList::SimpleTypeName),
        1806 => Reduction::SimpleTypeNameList(SimpleTypeNameList::SimpleTypeNameList_COMMA_SimpleTypeName),
        1807 => Reduction::SimpleUpdate(SimpleUpdate::UPDATE_Expr_OptFilterClause_SET_Shape),
        1808 => Reduction::SingleStatement(SingleStatement::ConfigStmt),
        1809 => Reduction::SingleStatement(SingleStatement::DDLStmt),
        1810 => Reduction::SingleStatement(SingleStatement::IfThenElseExpr),
        1811 => Reduction::SingleStatement(SingleStatement::SessionStmt),
        1812 => Reduction::SingleStatement(SingleStatement::Stmt),
        1813 => Reduction::SortClause(SortClause::ORDERBY_OrderbyList),
        1814 => Reduction::Splat(Splat::DOUBLESTAR),
        1815 => Reduction::Splat(Splat::ParenTypeExpr_DOT_DOUBLESTAR),
        1816 => Reduction::Splat(Splat::ParenTypeExpr_DOT_STAR),
        1817 => Reduction::Splat(Splat::ParenTypeExpr_TypeIntersection_DOT_DOUBLESTAR),
        1818 => Reduction::Splat(Splat::ParenTypeExpr_TypeIntersection_DOT_STAR),
        1819 => Reduction::Splat(Splat::PathStepName_DOT_DOUBLESTAR),
        1820 => Reduction::Splat(Splat::PathStepName_DOT_STAR),
        1821 => Reduction::Splat(Splat::PathStepName_TypeIntersection_DOT_DOUBLESTAR),
        1822 => Reduction::Splat(Splat::PathStepName_TypeIntersection_DOT_STAR),
        1823 => Reduction::Splat(Splat::PtrQualifiedNodeName_DOT_DOUBLESTAR),
        1824 => Reduction::Splat(Splat::PtrQualifiedNodeName_DOT_STAR),
        1825 => Reduction::Splat(Splat::PtrQualifiedNodeName_TypeIntersection_DOT_DOUBLESTAR),
        1826 => Reduction::Splat(Splat::PtrQualifiedNodeName_TypeIntersection_DOT_STAR),
        1827 => Reduction::Splat(Splat::STAR),
        1828 => Reduction::Splat(Splat::TypeIntersection_DOT_DOUBLESTAR),
        1829 => Reduction::Splat(Splat::TypeIntersection_DOT_STAR),
        1830 => Reduction::StartMigrationStmt(StartMigrationStmt::START_MIGRATION_TO_SDLCommandBlock),
        1831 => Reduction::StartMigrationStmt(StartMigrationStmt::START_MIGRATION_REWRITE),
        1832 => Reduction::StartMigrationStmt(StartMigrationStmt::START_MIGRATION_TO_COMMITTED_SCHEMA),
        1833 => Reduction::StatementBlock(StatementBlock::SingleStatement),
        1834 => Reduction::StatementBlock(StatementBlock::StatementBlock_Semicolons_SingleStatement),
        1835 => Reduction::Stmt(Stmt::AdministerStmt),
        1836 => Reduction::Stmt(Stmt::AnalyzeStmt),
        1837 => Reduction::Stmt(Stmt::DescribeStmt),
        1838 => Reduction::Stmt(Stmt::ExprStmt),
        1839 => Reduction::Stmt(Stmt::TransactionStmt),
        1840 => Reduction::Subtype(Subtype::BaseNumberConstant),
        1841 => Reduction::Subtype(Subtype::BaseStringConstant),
        1842 => Reduction::Subtype(Subtype::FullTypeExpr),
        1843 => Reduction::Subtype(Subtype::Identifier_COLON_FullTypeExpr),
        1844 => Reduction::SubtypeList(SubtypeList::SubtypeListInner),
        1845 => Reduction::SubtypeList(SubtypeList::SubtypeListInner_COMMA),
        1846 => Reduction::SubtypeListInner(SubtypeListInner::Subtype),
        1847 => Reduction::SubtypeListInner(SubtypeListInner::SubtypeListInner_COMMA_Subtype),
        1848 => Reduction::TransactionMode(TransactionMode::DEFERRABLE),
        1849 => Reduction::TransactionMode(TransactionMode::ISOLATION_SERIALIZABLE),
        1850 => Reduction::TransactionMode(TransactionMode::NOT_DEFERRABLE),
        1851 => Reduction::TransactionMode(TransactionMode::READ_ONLY),
        1852 => Reduction::TransactionMode(TransactionMode::READ_WRITE),
        1853 => Reduction::TransactionModeList(TransactionModeList::TransactionMode),
        1854 => Reduction::TransactionModeList(TransactionModeList::TransactionModeList_COMMA_TransactionMode),
        1855 => Reduction::TransactionStmt(TransactionStmt::COMMIT),
        1856 => Reduction::TransactionStmt(TransactionStmt::DECLARE_SAVEPOINT_Identifier),
        1857 => Reduction::TransactionStmt(TransactionStmt::RELEASE_SAVEPOINT_Identifier),
        1858 => Reduction::TransactionStmt(TransactionStmt::ROLLBACK),
        1859 => Reduction::TransactionStmt(TransactionStmt::ROLLBACK_TO_SAVEPOINT_Identifier),
        1860 => Reduction::TransactionStmt(TransactionStmt::START_TRANSACTION_OptTransactionModeList),
        1861 => Reduction::TriggerDeclarationBlock(TriggerDeclarationBlock::TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_CreateTriggerSDLCommandsBlock),
        1862 => Reduction::TriggerDeclarationShort(TriggerDeclarationShort::TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr),
        1863 => Reduction::TriggerKind(TriggerKind::DELETE),
        1864 => Reduction::TriggerKind(TriggerKind::INSERT),
        1865 => Reduction::TriggerKind(TriggerKind::UPDATE),
        1866 => Reduction::TriggerKindList(TriggerKindList::TriggerKind),
        1867 => Reduction::TriggerKindList(TriggerKindList::TriggerKindList_COMMA_TriggerKind),
        1868 => Reduction::TriggerScope(TriggerScope::ALL),
        1869 => Reduction::TriggerScope(TriggerScope::EACH),
        1870 => Reduction::TriggerTiming(TriggerTiming::AFTER),
        1871 => Reduction::TriggerTiming(TriggerTiming::AFTER_COMMIT_OF),
        1872 => Reduction::Tuple(Tuple::LPAREN_Expr_COMMA_OptExprList_RPAREN),
        1873 => Reduction::Tuple(Tuple::LPAREN_RPAREN),
        1874 => Reduction::TypeExpr(TypeExpr::NontrivialTypeExpr),
        1875 => Reduction::TypeExpr(TypeExpr::SimpleTypeName),
        1876 => Reduction::TypeIntersection(TypeIntersection::LBRACKET_IS_FullTypeExpr_RBRACKET),
        1877 => Reduction::TypeName(TypeName::CollectionTypeName),
        1878 => Reduction::TypeName(TypeName::SimpleTypeName),
        1879 => Reduction::TypeNameList(TypeNameList::TypeName),
        1880 => Reduction::TypeNameList(TypeNameList::TypeNameList_COMMA_TypeName),
        1881 => Reduction::UnlessConflictCause(UnlessConflictCause::UNLESS_CONFLICT_UnlessConflictSpecifier),
        1882 => Reduction::UnlessConflictSpecifier(UnlessConflictSpecifier::ON_Expr),
        1883 => Reduction::UnlessConflictSpecifier(UnlessConflictSpecifier::ON_Expr_ELSE_Expr),
        1884 => Reduction::UnlessConflictSpecifier(UnlessConflictSpecifier::epsilon),
        1885 => Reduction::UnqualifiedPointerName(UnqualifiedPointerName::PointerName),
        1886 => Reduction::UnreservedKeyword(UnreservedKeyword::ABORT),
        1887 => Reduction::UnreservedKeyword(UnreservedKeyword::ABSTRACT),
        1888 => Reduction::UnreservedKeyword(UnreservedKeyword::ACCESS),
        1889 => Reduction::UnreservedKeyword(UnreservedKeyword::AFTER),
        1890 => Reduction::UnreservedKeyword(UnreservedKeyword::ALIAS),
        1891 => Reduction::UnreservedKeyword(UnreservedKeyword::ALL),
        1892 => Reduction::UnreservedKeyword(UnreservedKeyword::ALLOW),
        1893 => Reduction::UnreservedKeyword(UnreservedKeyword::ANNOTATION),
        1894 => Reduction::UnreservedKeyword(UnreservedKeyword::APPLIED),
        1895 => Reduction::UnreservedKeyword(UnreservedKeyword::AS),
        1896 => Reduction::UnreservedKeyword(UnreservedKeyword::ASC),
        1897 => Reduction::UnreservedKeyword(UnreservedKeyword::ASSIGNMENT),
        1898 => Reduction::UnreservedKeyword(UnreservedKeyword::BEFORE),
        1899 => Reduction::UnreservedKeyword(UnreservedKeyword::BLOBAL),
        1900 => Reduction::UnreservedKeyword(UnreservedKeyword::BRANCH),
        1901 => Reduction::UnreservedKeyword(UnreservedKeyword::CARDINALITY),
        1902 => Reduction::UnreservedKeyword(UnreservedKeyword::CAST),
        1903 => Reduction::UnreservedKeyword(UnreservedKeyword::COMMITTED),
        1904 => Reduction::UnreservedKeyword(UnreservedKeyword::CONFIG),
        1905 => Reduction::UnreservedKeyword(UnreservedKeyword::CONFLICT),
        1906 => Reduction::UnreservedKeyword(UnreservedKeyword::CONSTRAINT),
        1907 => Reduction::UnreservedKeyword(UnreservedKeyword::CUBE),
        1908 => Reduction::UnreservedKeyword(UnreservedKeyword::CURRENT),
        1909 => Reduction::UnreservedKeyword(UnreservedKeyword::DATA),
        1910 => Reduction::UnreservedKeyword(UnreservedKeyword::DATABASE),
        1911 => Reduction::UnreservedKeyword(UnreservedKeyword::DDL),
        1912 => Reduction::UnreservedKeyword(UnreservedKeyword::DECLARE),
        1913 => Reduction::UnreservedKeyword(UnreservedKeyword::DEFAULT),
        1914 => Reduction::UnreservedKeyword(UnreservedKeyword::DEFERRABLE),
        1915 => Reduction::UnreservedKeyword(UnreservedKeyword::DEFERRED),
        1916 => Reduction::UnreservedKeyword(UnreservedKeyword::DELEGATED),
        1917 => Reduction::UnreservedKeyword(UnreservedKeyword::DENY),
        1918 => Reduction::UnreservedKeyword(UnreservedKeyword::DESC),
        1919 => Reduction::UnreservedKeyword(UnreservedKeyword::EACH),
        1920 => Reduction::UnreservedKeyword(UnreservedKeyword::EMPTY),
        1921 => Reduction::UnreservedKeyword(UnreservedKeyword::EXPRESSION),
        1922 => Reduction::UnreservedKeyword(UnreservedKeyword::EXTENSION),
        1923 => Reduction::UnreservedKeyword(UnreservedKeyword::FINAL),
        1924 => Reduction::UnreservedKeyword(UnreservedKeyword::FIRST),
        1925 => Reduction::UnreservedKeyword(UnreservedKeyword::FORCE),
        1926 => Reduction::UnreservedKeyword(UnreservedKeyword::FROM),
        1927 => Reduction::UnreservedKeyword(UnreservedKeyword::FUNCTION),
        1928 => Reduction::UnreservedKeyword(UnreservedKeyword::FUTURE),
        1929 => Reduction::UnreservedKeyword(UnreservedKeyword::IMPLICIT),
        1930 => Reduction::UnreservedKeyword(UnreservedKeyword::INDEX),
        1931 => Reduction::UnreservedKeyword(UnreservedKeyword::INFIX),
        1932 => Reduction::UnreservedKeyword(UnreservedKeyword::INHERITABLE),
        1933 => Reduction::UnreservedKeyword(UnreservedKeyword::INSTANCE),
        1934 => Reduction::UnreservedKeyword(UnreservedKeyword::INTO),
        1935 => Reduction::UnreservedKeyword(UnreservedKeyword::ISOLATION),
        1936 => Reduction::UnreservedKeyword(UnreservedKeyword::JSON),
        1937 => Reduction::UnreservedKeyword(UnreservedKeyword::LAST),
        1938 => Reduction::UnreservedKeyword(UnreservedKeyword::LINK),
        1939 => Reduction::UnreservedKeyword(UnreservedKeyword::MIGRATION),
        1940 => Reduction::UnreservedKeyword(UnreservedKeyword::MULTI),
        1941 => Reduction::UnreservedKeyword(UnreservedKeyword::NAMED),
        1942 => Reduction::UnreservedKeyword(UnreservedKeyword::OBJECT),
        1943 => Reduction::UnreservedKeyword(UnreservedKeyword::OF),
        1944 => Reduction::UnreservedKeyword(UnreservedKeyword::ONLY),
        1945 => Reduction::UnreservedKeyword(UnreservedKeyword::ONTO),
        1946 => Reduction::UnreservedKeyword(UnreservedKeyword::OPERATOR),
        1947 => Reduction::UnreservedKeyword(UnreservedKeyword::OPTIONALITY),
        1948 => Reduction::UnreservedKeyword(UnreservedKeyword::ORDER),
        1949 => Reduction::UnreservedKeyword(UnreservedKeyword::ORPHAN),
        1950 => Reduction::UnreservedKeyword(UnreservedKeyword::OVERLOADED),
        1951 => Reduction::UnreservedKeyword(UnreservedKeyword::OWNED),
        1952 => Reduction::UnreservedKeyword(UnreservedKeyword::PACKAGE),
        1953 => Reduction::UnreservedKeyword(UnreservedKeyword::POLICY),
        1954 => Reduction::UnreservedKeyword(UnreservedKeyword::POPULATE),
        1955 => Reduction::UnreservedKeyword(UnreservedKeyword::POSTFIX),
        1956 => Reduction::UnreservedKeyword(UnreservedKeyword::PREFIX),
        1957 => Reduction::UnreservedKeyword(UnreservedKeyword::PROPERTY),
        1958 => Reduction::UnreservedKeyword(UnreservedKeyword::PROPOSED),
        1959 => Reduction::UnreservedKeyword(UnreservedKeyword::PSEUDO),
        1960 => Reduction::UnreservedKeyword(UnreservedKeyword::READ),
        1961 => Reduction::UnreservedKeyword(UnreservedKeyword::REJECT),
        1962 => Reduction::UnreservedKeyword(UnreservedKeyword::RELEASE),
        1963 => Reduction::UnreservedKeyword(UnreservedKeyword::RENAME),
        1964 => Reduction::UnreservedKeyword(UnreservedKeyword::REQUIRED),
        1965 => Reduction::UnreservedKeyword(UnreservedKeyword::RESET),
        1966 => Reduction::UnreservedKeyword(UnreservedKeyword::RESTRICT),
        1967 => Reduction::UnreservedKeyword(UnreservedKeyword::REWRITE),
        1968 => Reduction::UnreservedKeyword(UnreservedKeyword::ROLE),
        1969 => Reduction::UnreservedKeyword(UnreservedKeyword::ROLES),
        1970 => Reduction::UnreservedKeyword(UnreservedKeyword::ROLLUP),
        1971 => Reduction::UnreservedKeyword(UnreservedKeyword::SAVEPOINT),
        1972 => Reduction::UnreservedKeyword(UnreservedKeyword::SCALAR),
        1973 => Reduction::UnreservedKeyword(UnreservedKeyword::SCHEMA),
        1974 => Reduction::UnreservedKeyword(UnreservedKeyword::SDL),
        1975 => Reduction::UnreservedKeyword(UnreservedKeyword::SERIALIZABLE),
        1976 => Reduction::UnreservedKeyword(UnreservedKeyword::SESSION),
        1977 => Reduction::UnreservedKeyword(UnreservedKeyword::SOURCE),
        1978 => Reduction::UnreservedKeyword(UnreservedKeyword::SUPERUSER),
        1979 => Reduction::UnreservedKeyword(UnreservedKeyword::SYSTEM),
        1980 => Reduction::UnreservedKeyword(UnreservedKeyword::TARGET),
        1981 => Reduction::UnreservedKeyword(UnreservedKeyword::TEMPLATE),
        1982 => Reduction::UnreservedKeyword(UnreservedKeyword::TERNARY),
        1983 => Reduction::UnreservedKeyword(UnreservedKeyword::TEXT),
        1984 => Reduction::UnreservedKeyword(UnreservedKeyword::THEN),
        1985 => Reduction::UnreservedKeyword(UnreservedKeyword::TO),
        1986 => Reduction::UnreservedKeyword(UnreservedKeyword::TRANSACTION),
        1987 => Reduction::UnreservedKeyword(UnreservedKeyword::TRIGGER),
        1988 => Reduction::UnreservedKeyword(UnreservedKeyword::TYPE),
        1989 => Reduction::UnreservedKeyword(UnreservedKeyword::UNLESS),
        1990 => Reduction::UnreservedKeyword(UnreservedKeyword::USING),
        1991 => Reduction::UnreservedKeyword(UnreservedKeyword::VERBOSE),
        1992 => Reduction::UnreservedKeyword(UnreservedKeyword::VERSION),
        1993 => Reduction::UnreservedKeyword(UnreservedKeyword::VIEW),
        1994 => Reduction::UnreservedKeyword(UnreservedKeyword::WRITE),
        1995 => Reduction::Using(Using::USING_ParenExpr),
        1996 => Reduction::UsingClause(UsingClause::USING_AliasedExprList),
        1997 => Reduction::UsingStmt(UsingStmt::RESET_EXPRESSION),
        1998 => Reduction::UsingStmt(UsingStmt::USING_ParenExpr),
        1999 => Reduction::WithBlock(WithBlock::WITH_WithDeclList),
        2000 => Reduction::WithDDLStmt(WithDDLStmt::InnerDDLStmt),
        2001 => Reduction::WithDecl(WithDecl::AliasDecl),
        2002 => Reduction::WithDeclList(WithDeclList::WithDeclListInner),
        2003 => Reduction::WithDeclList(WithDeclList::WithDeclListInner_COMMA),
        2004 => Reduction::WithDeclListInner(WithDeclListInner::WithDecl),
        2005 => Reduction::WithDeclListInner(WithDeclListInner::WithDeclListInner_COMMA_WithDecl),
        _ => unreachable!(),
    }
}
