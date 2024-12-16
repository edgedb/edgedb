use super::*;
use crate::parser::CSTNode;
use crate::ast;

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AbortMigrationStmt {
    ABORT_MIGRATION,
    ABORT_MIGRATION_REWRITE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AccessPermStmt {
    AccessPolicyAction_AccessKindList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AccessPolicyAction {
    ALLOW,
    DENY,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AccessPolicyDeclarationBlock {
    ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_CreateAccessPolicySDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AccessPolicyDeclarationShort {
    ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AccessUsingStmt {
    RESET_EXPRESSION,
    USING_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AccessWhenStmt {
    RESET_WHEN,
    WHEN_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::AdministerStmt)]
#[stub()]
pub enum AdministerStmt {
    ADMINISTER_FuncExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AliasDecl {
    AliasedExpr,
    Identifier_AS_MODULE_ModuleName,
    MODULE_ModuleName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AliasDeclaration {
    ALIAS_NodeName_CreateAliasSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AliasDeclarationShort {
    ALIAS_NodeName_CreateAliasSingleSDLCommandBlock,
    ALIAS_NodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::AliasedExpr)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum AliasedExpr {
    Identifier_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAbstract {
    DROP_ABSTRACT,
    RESET_ABSTRACT,
    SET_ABSTRACT,
    SET_NOT_ABSTRACT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterAccessPolicyCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAccessPolicyCommandsBlock {
    AlterAccessPolicyCommand,
    LBRACE_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAccessPolicyStmt {
    ALTER_ACCESS_POLICY_UnqualifiedPointerName_AlterAccessPolicyCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterAliasCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterAliasCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAliasCommandsBlock {
    AlterAliasCommand,
    LBRACE_AlterAliasCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterAliasCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAliasStmt {
    ALTER_ALIAS_NodeName_AlterAliasCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterAnnotationCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterAnnotationCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAnnotationCommandsBlock {
    AlterAnnotationCommand,
    LBRACE_AlterAnnotationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterAnnotationCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAnnotationStmt {
    ALTER_ABSTRACT_ANNOTATION_NodeName_AlterAnnotationCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterAnnotationValueStmt {
    ALTER_ANNOTATION_NodeName_ASSIGN_Expr,
    ALTER_ANNOTATION_NodeName_DROP_OWNED,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterBranchCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterBranchCommand {
    RenameStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterBranchCommandsBlock {
    AlterBranchCommand,
    LBRACE_AlterBranchCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterBranchCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterBranchStmt {
    ALTER_BRANCH_DatabaseName_BranchOptions_AlterBranchCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterCastCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterCastCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterCastCommandsBlock {
    AlterCastCommand,
    LBRACE_AlterCastCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterCastCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterCastStmt {
    ALTER_CAST_FROM_TypeName_TO_TypeName_AlterCastCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterCommandsBlock {
    AlterCommand,
    LBRACE_AlterCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterConcreteConstraintCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcreteConstraintCommandsBlock {
    AlterConcreteConstraintCommand,
    LBRACE_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcreteConstraintStmt {
    ALTER_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_AlterConcreteConstraintCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterConcreteIndexCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterConcreteIndexCommand {
    AlterAnnotationValueStmt,
    AlterDeferredStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcreteIndexCommandsBlock {
    AlterConcreteIndexCommand,
    LBRACE_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcreteIndexStmt {
    ALTER_INDEX_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock,
    ALTER_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterConcreteLinkCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcreteLinkCommandsBlock {
    AlterConcreteLinkCommand,
    LBRACE_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcreteLinkStmt {
    ALTER_LINK_UnqualifiedPointerName_AlterConcreteLinkCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterConcretePropertyCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcretePropertyCommandsBlock {
    AlterConcretePropertyCommand,
    LBRACE_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConcretePropertyStmt {
    ALTER_PROPERTY_UnqualifiedPointerName_AlterConcretePropertyCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterConstraintStmt {
    ALTER_ABSTRACT_CONSTRAINT_NodeName_AlterCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterCurrentMigrationStmt {
    ALTER_CURRENT_MIGRATION_REJECT_PROPOSED,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterDatabaseCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterDatabaseCommand {
    RenameStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterDatabaseCommandsBlock {
    AlterDatabaseCommand,
    LBRACE_AlterDatabaseCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterDatabaseCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterDatabaseStmt {
    ALTER_DATABASE_DatabaseName_AlterDatabaseCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterDeferredStmt {
    DROP_DEFERRED,
    SET_DEFERRED,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterExtending {
    AlterAbstract,
    DROP_EXTENDING_TypeNameList,
    EXTENDING_TypeNameList_OptPosition,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterExtensionStmt {
    ALTER_EXTENSION_ShortNodeName_TO_ExtensionVersion,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterFunctionCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterFunctionCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    FromFunction,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterFunctionCommandsBlock {
    AlterFunctionCommand,
    LBRACE_AlterFunctionCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterFunctionCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterFunctionStmt {
    ALTER_FUNCTION_NodeName_CreateFunctionArgs_AlterFunctionCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterGlobalCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterGlobalCommandsBlock {
    AlterGlobalCommand,
    LBRACE_AlterGlobalCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterGlobalCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterGlobalStmt {
    ALTER_GLOBAL_NodeName_AlterGlobalCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterIndexCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterIndexCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterIndexCommandsBlock {
    AlterIndexCommand,
    LBRACE_AlterIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterIndexStmt {
    ALTER_ABSTRACT_INDEX_NodeName_AlterIndexCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterLinkCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterLinkCommandsBlock {
    AlterLinkCommand,
    LBRACE_AlterLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterLinkStmt {
    ALTER_ABSTRACT_LINK_PtrNodeName_AlterLinkCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterMigrationCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterMigrationCommand {
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterMigrationCommandsBlock {
    AlterMigrationCommand,
    LBRACE_AlterMigrationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterMigrationCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterMigrationStmt {
    ALTER_MIGRATION_NodeName_AlterMigrationCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterModuleStmt {
    ALTER_MODULE_ModuleName_AlterCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterObjectTypeCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterObjectTypeCommandsBlock {
    AlterObjectTypeCommand,
    LBRACE_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterObjectTypeStmt {
    ALTER_TYPE_NodeName_AlterObjectTypeCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterOperatorCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterOperatorCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterOperatorCommandsBlock {
    AlterOperatorCommand,
    LBRACE_AlterOperatorCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterOperatorCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterOperatorStmt {
    ALTER_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_AlterOperatorCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterOwnedStmt {
    DROP_OWNED,
    SET_OWNED,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterPropertyCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterPropertyCommandsBlock {
    AlterPropertyCommand,
    LBRACE_AlterPropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterPropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterPropertyStmt {
    ALTER_ABSTRACT_PROPERTY_PtrNodeName_AlterPropertyCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterRewriteCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterRewriteCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    DropAnnotationValueStmt,
    ResetFieldStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterRewriteCommandsBlock {
    AlterRewriteCommand,
    LBRACE_AlterRewriteCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterRewriteCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterRewriteStmt {
    ALTER_REWRITE_RewriteKindList_AlterRewriteCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterRoleCommandsList, separator=Semicolons)]
#[stub()]
pub enum AlterRoleCommand {
    AlterRoleExtending,
    RenameStmt,
    ResetFieldStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterRoleCommandsBlock {
    AlterRoleCommand,
    LBRACE_AlterRoleCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterRoleCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterRoleExtending {
    DROP_EXTENDING_ShortNodeNameList,
    EXTENDING_ShortNodeNameList_OptPosition,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterRoleStmt {
    ALTER_ROLE_ShortNodeName_AlterRoleCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterScalarTypeCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterScalarTypeCommandsBlock {
    AlterScalarTypeCommand,
    LBRACE_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterScalarTypeStmt {
    ALTER_SCALAR_TYPE_NodeName_AlterScalarTypeCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterSimpleExtending {
    AlterAbstract,
    DROP_EXTENDING_SimpleTypeNameList,
    EXTENDING_SimpleTypeNameList_OptPosition,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=AlterTriggerCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterTriggerCommandsBlock {
    AlterTriggerCommand,
    LBRACE_AlterTriggerCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_AlterTriggerCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AlterTriggerStmt {
    ALTER_TRIGGER_UnqualifiedPointerName_AlterTriggerCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::ExplainStmt)]
#[stub()]
pub enum AnalyzeStmt {
    ANALYZE_ExprStmt,
    ANALYZE_NamedTuple_ExprStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AnnotationDeclaration {
    ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
    ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AnnotationDeclarationShort {
    ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple,
    ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AnyIdentifier {
    PtrIdentifier,
    ReservedKeyword,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AnyNodeName {
    AnyIdentifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AtomicExpr {
    AtomicPath,
    BaseAtomicExpr,
    LANGBRACKET_FullTypeExpr_RANGBRACKET_AtomicExpr_P_TYPECAST,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum AtomicPath {
    AtomicExpr_PathStep_P_DOT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BaseBooleanConstant {
    FALSE,
    TRUE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BaseBytesConstant {
    BCONST,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BaseName {
    Identifier,
    QualifiedName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BaseNumberConstant {
    FCONST,
    ICONST,
    NFCONST,
    NICONST,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BaseStringConstant {
    SCONST,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BranchOptions {
    FORCE,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum BranchStmt {
    AlterBranchStmt,
    CreateBranchStmt,
    DropBranchStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(Vec::<ast::GroupingElement>)]
#[stub()]
pub enum ByClause {
    BY_GroupingElementList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CastAllowedUse {
    ALLOW_ASSIGNMENT,
    ALLOW_IMPLICIT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CastCode {
    USING_Identifier_BaseStringConstant,
    USING_Identifier_CAST,
    USING_Identifier_EXPRESSION,
    USING_Identifier_FUNCTION_BaseStringConstant,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Collection {
    LBRACKET_OptExprList_RBRACKET,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CollectionTypeName {
    NodeName_LANGBRACKET_RANGBRACKET,
    NodeName_LANGBRACKET_SubtypeList_RANGBRACKET,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ColonedIdents {
    AnyIdentifier,
    ColonedIdents_DOUBLECOLON_AnyIdentifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CommitMigrationStmt {
    COMMIT_MIGRATION,
    COMMIT_MIGRATION_REWRITE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteConstraintBlock {
    CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
    DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteConstraintShort {
    CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
    DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteIndexDeclarationBlock {
    DEFERRED_INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
    INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteIndexDeclarationShort {
    DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr,
    DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
    INDEX_NodeName_OnExpr_OptExceptExpr,
    INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
    DEFERRED_INDEX_OnExpr_OptExceptExpr,
    INDEX_OnExpr_OptExceptExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteLinkBlock {
    OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteLinkShort {
    LINK_PathNodeName_ASSIGN_Expr,
    OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
    OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
    PtrQuals_LINK_PathNodeName_ASSIGN_Expr,
    LINK_PathNodeName_OptExtendingSimple_PtrTarget,
    PtrQuals_LINK_PathNodeName_OptExtendingSimple_PtrTarget,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcretePropertyBlock {
    OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
    OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
    PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
    PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcretePropertyShort {
    PROPERTY_PathNodeName_ASSIGN_Expr,
    OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
    OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
    PtrQuals_PROPERTY_PathNodeName_ASSIGN_Expr,
    PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
    PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteUnknownPointerBlock {
    OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
    PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteUnknownPointerObjectShort {
    PathNodeName_ASSIGN_Expr,
    PtrQuals_PathNodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConcreteUnknownPointerShort {
    OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget,
    OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget,
    PathNodeName_OptExtendingSimple_PtrTarget,
    PtrQuals_PathNodeName_OptExtendingSimple_PtrTarget,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::ConfigOp)]
#[stub()]
pub enum ConfigOp {
    INSERT_NodeName_Shape,
    RESET_NodeName_OptFilterClause,
    SET_NodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConfigScope {
    CURRENT_BRANCH,
    CURRENT_DATABASE,
    INSTANCE,
    SESSION,
    SYSTEM,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConfigStmt {
    CONFIGURE_BRANCH_ConfigOp,
    CONFIGURE_ConfigScope_ConfigOp,
    CONFIGURE_DATABASE_ConfigOp,
    RESET_GLOBAL_NodeName,
    SET_GLOBAL_NodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Constant {
    BaseBooleanConstant,
    BaseBytesConstant,
    BaseNumberConstant,
    BaseStringConstant,
    PARAMETER,
    PARAMETERANDTYPE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConstraintDeclaration {
    ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
    ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ConstraintDeclarationShort {
    ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple,
    ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateAccessPolicyCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateAccessPolicyCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAccessPolicyCommandsBlock {
    LBRACE_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateAccessPolicySDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateAccessPolicySDLCommandFull {
    CreateAccessPolicySDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAccessPolicySDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAccessPolicySDLCommandsBlock {
    LBRACE_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAccessPolicyStmt {
    CREATE_ACCESS_POLICY_UnqualifiedPointerName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_OptCreateAccessPolicyCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateAliasCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateAliasCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAliasCommandsBlock {
    CreateAliasCommand,
    LBRACE_CreateAliasCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateAliasCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateAliasSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateAliasSDLCommandFull {
    CreateAliasSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAliasSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAliasSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAliasSingleSDLCommandBlock {
    CreateAliasSDLCommandShort,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAliasStmt {
    CREATE_ALIAS_NodeName_CreateAliasCommandsBlock,
    CREATE_ALIAS_NodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateAnnotationCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateAnnotationCommand {
    CreateAnnotationValueStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAnnotationCommandsBlock {
    LBRACE_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAnnotationStmt {
    CREATE_ABSTRACT_ANNOTATION_NodeName_OptCreateAnnotationCommandsBlock,
    CREATE_ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptCreateCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateAnnotationValueStmt {
    CREATE_ANNOTATION_NodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateBranchStmt {
    CREATE_EMPTY_BRANCH_DatabaseName,
    CREATE_DATA_BRANCH_DatabaseName_FROM_DatabaseName,
    CREATE_SCHEMA_BRANCH_DatabaseName_FROM_DatabaseName,
    CREATE_TEMPLATE_BRANCH_DatabaseName_FROM_DatabaseName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateCastCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateCastCommand {
    AlterAnnotationValueStmt,
    CastAllowedUse,
    CastCode,
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateCastCommandsBlock {
    CreateCastCommand,
    LBRACE_CreateCastCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateCastCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateCastStmt {
    CREATE_CAST_FROM_TypeName_TO_TypeName_CreateCastCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateCommandsBlock {
    LBRACE_CreateCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteConstraintStmt {
    CREATE_OptDelegated_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_OptCreateCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateConcreteIndexSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateConcreteIndexSDLCommandFull {
    CreateConcreteIndexSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteIndexSDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteIndexSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteIndexStmt {
    CREATE_OptDeferred_INDEX_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
    CREATE_OptDeferred_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateConcreteLinkCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteLinkCommandsBlock {
    LBRACE_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteLinkSDLCommandBlock {
    ConcreteConstraintBlock,
    ConcreteIndexDeclarationBlock,
    ConcretePropertyBlock,
    ConcreteUnknownPointerBlock,
    RewriteDeclarationBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateConcreteLinkSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateConcreteLinkSDLCommandFull {
    CreateConcreteLinkSDLCommandBlock,
    CreateConcreteLinkSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteLinkSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcreteLinkStmt {
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_ASSIGN_Expr,
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptCreateConcreteLinkCommandsBlock,
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
    CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateConcretePropertyCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcretePropertyCommandsBlock {
    LBRACE_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcretePropertySDLCommandBlock {
    ConcreteConstraintBlock,
    RewriteDeclarationBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateConcretePropertySDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateConcretePropertySDLCommandFull {
    CreateConcretePropertySDLCommandBlock,
    CreateConcretePropertySDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcretePropertySDLCommandShort {
    ConcreteConstraintShort,
    CreateSimpleExtending,
    RewriteDeclarationShort,
    SetAnnotation,
    SetField,
    Using,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcretePropertySDLCommandsBlock {
    LBRACE_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConcretePropertyStmt {
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_ASSIGN_Expr,
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptCreateConcretePropertyCommandsBlock,
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
    CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateConstraintStmt {
    CREATE_ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
    CREATE_ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateDatabaseCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateDatabaseCommand {
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateDatabaseCommandsBlock {
    LBRACE_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateDatabaseStmt {
    CREATE_DATABASE_DatabaseName_FROM_AnyNodeName_OptCreateDatabaseCommandsBlock,
    CREATE_DATABASE_DatabaseName_OptCreateDatabaseCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateExtensionCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateExtensionCommand {
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateExtensionCommandsBlock {
    LBRACE_CreateExtensionCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateExtensionCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateExtensionPackageCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateExtensionPackageCommand {
    NestedQLBlockStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateExtensionPackageCommandsBlock {
    LBRACE_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateExtensionPackageMigrationStmt {
    CREATE_EXTENSIONPACKAGE_ShortNodeName_MIGRATION_FROM_ExtensionVersion_TO_ExtensionVersion_OptCreateExtensionPackageCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateExtensionPackageStmt {
    CREATE_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion_OptCreateExtensionPackageCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateExtensionStmt {
    CREATE_EXTENSION_ShortNodeName_OptExtensionVersion_OptCreateExtensionCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFunctionArgs {
    LPAREN_FuncDeclArgs_RPAREN,
    LPAREN_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateFunctionCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateFunctionCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    FromFunction,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFunctionCommandsBlock {
    CreateFunctionCommand,
    LBRACE_CreateFunctionCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateFunctionCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateFunctionSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateFunctionSDLCommandFull {
    CreateFunctionSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFunctionSDLCommandShort {
    FromFunction,
    SetAnnotation,
    SetField,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFunctionSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFunctionSingleSDLCommandBlock {
    CreateFunctionSDLCommandShort,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFunctionStmt {
    CREATE_FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateFutureStmt {
    CREATE_FUTURE_ShortNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateGlobalCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateGlobalCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateGlobalCommandsBlock {
    LBRACE_CreateGlobalCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateGlobalCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateGlobalSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateGlobalSDLCommandFull {
    CreateGlobalSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateGlobalSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateGlobalSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateGlobalStmt {
    CREATE_OptPtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
    CREATE_OptPtrQuals_GLOBAL_NodeName_OptCreateConcretePropertyCommandsBlock,
    CREATE_OptPtrQuals_GLOBAL_NodeName_ARROW_FullTypeExpr_OptCreateGlobalCommandsBlock,
    CREATE_OptPtrQuals_GLOBAL_NodeName_COLON_FullTypeExpr_OptCreateGlobalCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateIndexCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateIndexCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateIndexCommandsBlock {
    LBRACE_CreateIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateIndexMatchCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateIndexMatchCommand {
    CreateAnnotationValueStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateIndexMatchCommandsBlock {
    LBRACE_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateIndexMatchStmt {
    CREATE_INDEX_MATCH_FOR_TypeName_USING_NodeName_OptCreateIndexMatchCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateIndexSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateIndexSDLCommandFull {
    CreateIndexSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateIndexSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateIndexSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateIndexStmt {
    CREATE_ABSTRACT_INDEX_NodeName_OptExtendingSimple_OptCreateIndexCommandsBlock,
    CREATE_ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_OptCreateIndexCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateLinkCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateLinkCommandsBlock {
    LBRACE_CreateLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateLinkSDLCommandBlock {
    ConcreteConstraintBlock,
    ConcreteIndexDeclarationBlock,
    ConcretePropertyBlock,
    ConcreteUnknownPointerBlock,
    RewriteDeclarationBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateLinkSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateLinkSDLCommandFull {
    CreateLinkSDLCommandBlock,
    CreateLinkSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateLinkSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateLinkStmt {
    CREATE_ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_OptCreateLinkCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateMigrationCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateMigrationCommand {
    NestedQLBlockStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateMigrationCommandsBlock {
    LBRACE_CreateMigrationCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateMigrationCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateMigrationStmt {
    CREATE_APPLIED_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
    CREATE_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateModuleStmt {
    CREATE_MODULE_ModuleName_OptIfNotExists_OptCreateCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateObjectTypeCommandsList, separator=Semicolons)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateObjectTypeCommandsBlock {
    LBRACE_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateObjectTypeSDLCommandBlock {
    AccessPolicyDeclarationBlock,
    ConcreteConstraintBlock,
    ConcreteIndexDeclarationBlock,
    ConcreteLinkBlock,
    ConcretePropertyBlock,
    ConcreteUnknownPointerBlock,
    TriggerDeclarationBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateObjectTypeSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateObjectTypeSDLCommandFull {
    CreateObjectTypeSDLCommandBlock,
    CreateObjectTypeSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateObjectTypeSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateObjectTypeStmt {
    CREATE_ABSTRACT_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
    CREATE_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateOperatorCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateOperatorCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    OperatorCode,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateOperatorCommandsBlock {
    CreateOperatorCommand,
    LBRACE_CreateOperatorCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateOperatorCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateOperatorStmt {
    CREATE_ABSTRACT_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_OptCreateOperatorCommandsBlock,
    CREATE_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateOperatorCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreatePropertyCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreatePropertyCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateSimpleExtending,
    SetFieldStmt,
    UsingStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreatePropertyCommandsBlock {
    LBRACE_CreatePropertyCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreatePropertyCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreatePropertySDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreatePropertySDLCommandFull {
    CreatePropertySDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreatePropertySDLCommandShort {
    CreateSimpleExtending,
    SetAnnotation,
    SetField,
    Using,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreatePropertySDLCommandsBlock {
    LBRACE_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreatePropertyStmt {
    CREATE_ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_OptCreatePropertyCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreatePseudoTypeCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreatePseudoTypeCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreatePseudoTypeCommandsBlock {
    LBRACE_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreatePseudoTypeStmt {
    CREATE_PSEUDO_TYPE_NodeName_OptCreatePseudoTypeCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateRewriteCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateRewriteCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateRewriteCommandsBlock {
    LBRACE_CreateRewriteCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateRewriteCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateRewriteSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateRewriteSDLCommandFull {
    CreateRewriteSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateRewriteSDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateRewriteSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateRewriteStmt {
    CREATE_REWRITE_RewriteKindList_USING_ParenExpr_OptCreateRewriteCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateRoleCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateRoleCommand {
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateRoleCommandsBlock {
    LBRACE_CreateRoleCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateRoleCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateRoleStmt {
    CREATE_OptSuperuser_ROLE_ShortNodeName_OptShortExtending_OptIfNotExists_OptCreateRoleCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateSDLCommandFull {
    CreateSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateSDLCommandShort {
    SetAnnotation,
    SetField,
    Using,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_CreateSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateScalarTypeCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateScalarTypeCommand {
    AlterAnnotationValueStmt,
    CreateAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateScalarTypeCommandsBlock {
    LBRACE_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateScalarTypeSDLCommandBlock {
    ConcreteConstraintBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateScalarTypeSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateScalarTypeSDLCommandFull {
    CreateScalarTypeSDLCommandBlock,
    CreateScalarTypeSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateScalarTypeSDLCommandShort {
    ConcreteConstraintShort,
    SetAnnotation,
    SetField,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateScalarTypeSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateScalarTypeStmt {
    CREATE_ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
    CREATE_FINAL_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
    CREATE_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateSimpleExtending {
    EXTENDING_SimpleTypeNameList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateTriggerCommandsList, separator=Semicolons)]
#[stub()]
pub enum CreateTriggerCommand {
    CreateAnnotationValueStmt,
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateTriggerCommandsBlock {
    LBRACE_CreateTriggerCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_CreateTriggerCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=CreateTriggerSDLCommandsList, separator=OptSemicolons)]
#[stub()]
pub enum CreateTriggerSDLCommandFull {
    CreateTriggerSDLCommandShort_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateTriggerSDLCommandShort {
    SetAnnotation,
    SetField,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateTriggerSDLCommandsBlock {
    LBRACE_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
    LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum CreateTriggerStmt {
    CREATE_TRIGGER_UnqualifiedPointerName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_OptCreateTriggerCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::DDLCommand)]
#[stub()]
pub enum DDLStmt {
    BranchStmt,
    DatabaseStmt,
    ExtensionPackageStmt,
    MigrationStmt,
    OptWithDDLStmt,
    RoleStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DDLWithBlock {
    WithBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DatabaseName {
    Identifier,
    ReservedKeyword,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DatabaseStmt {
    AlterDatabaseStmt,
    CreateDatabaseStmt,
    DropDatabaseStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DescribeFormat {
    AS_DDL,
    AS_JSON,
    AS_SDL,
    AS_TEXT,
    AS_TEXT_VERBOSE,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::DescribeStmt)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DotName {
    DottedIdents,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DottedIdents {
    AnyIdentifier,
    DottedIdents_DOT_AnyIdentifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropAccessPolicyStmt {
    DROP_ACCESS_POLICY_UnqualifiedPointerName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropAliasStmt {
    DROP_ALIAS_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropAnnotationStmt {
    DROP_ABSTRACT_ANNOTATION_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropAnnotationValueStmt {
    DROP_ANNOTATION_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropBranchStmt {
    DROP_BRANCH_DatabaseName_BranchOptions,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropCastStmt {
    DROP_CAST_FROM_TypeName_TO_TypeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConcreteConstraintStmt {
    DROP_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=DropConcreteIndexCommandsList, separator=Semicolons)]
#[stub()]
pub enum DropConcreteIndexCommand {
    SetFieldStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConcreteIndexCommandsBlock {
    LBRACE_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConcreteIndexStmt {
    DROP_INDEX_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
    DROP_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=DropConcreteLinkCommandsList, separator=Semicolons)]
#[stub()]
pub enum DropConcreteLinkCommand {
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcretePropertyStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConcreteLinkCommandsBlock {
    LBRACE_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConcreteLinkStmt {
    DROP_LINK_UnqualifiedPointerName_OptDropConcreteLinkCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConcretePropertyStmt {
    DROP_PROPERTY_UnqualifiedPointerName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropConstraintStmt {
    DROP_ABSTRACT_CONSTRAINT_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropDatabaseStmt {
    DROP_DATABASE_DatabaseName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropExtensionPackageMigrationStmt {
    DROP_EXTENSIONPACKAGE_ShortNodeName_MIGRATION_FROM_ExtensionVersion_TO_ExtensionVersion,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropExtensionPackageStmt {
    DROP_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropExtensionStmt {
    DROP_EXTENSION_ShortNodeName_OptExtensionVersion,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropFunctionStmt {
    DROP_FUNCTION_NodeName_CreateFunctionArgs,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropFutureStmt {
    DROP_FUTURE_ShortNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropGlobalStmt {
    DROP_GLOBAL_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropIndexMatchStmt {
    DROP_INDEX_MATCH_FOR_TypeName_USING_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropIndexStmt {
    DROP_ABSTRACT_INDEX_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=DropLinkCommandsList, separator=Semicolons)]
#[stub()]
pub enum DropLinkCommand {
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcretePropertyStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropLinkCommandsBlock {
    LBRACE_DropLinkCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropLinkCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropLinkStmt {
    DROP_ABSTRACT_LINK_PtrNodeName_OptDropLinkCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropMigrationStmt {
    DROP_MIGRATION_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropModuleStmt {
    DROP_MODULE_ModuleName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=DropObjectTypeCommandsList, separator=Semicolons)]
#[stub()]
pub enum DropObjectTypeCommand {
    DropConcreteConstraintStmt,
    DropConcreteIndexStmt,
    DropConcreteLinkStmt,
    DropConcretePropertyStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropObjectTypeCommandsBlock {
    LBRACE_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_Semicolons_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropObjectTypeStmt {
    DROP_TYPE_NodeName_OptDropObjectTypeCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropOperatorStmt {
    DROP_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropPropertyStmt {
    DROP_ABSTRACT_PROPERTY_PtrNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropRewriteStmt {
    DROP_REWRITE_RewriteKindList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropRoleStmt {
    DROP_ROLE_ShortNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropScalarTypeStmt {
    DROP_SCALAR_TYPE_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum DropTriggerStmt {
    DROP_TRIGGER_UnqualifiedPointerName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(Vec::<ast::Command>)]
#[stub()]
pub enum EdgeQLBlock {
    OptSemicolons,
    StatementBlock_OptSemicolons,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::Query)]
#[stub()]
pub enum ExprStmt {
    ExprStmtCore,
    WithBlock_ExprStmtCore,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::Query)]
#[stub()]
pub enum ExprStmtCore {
    InternalGroup,
    SimpleDelete,
    SimpleFor,
    SimpleGroup,
    SimpleInsert,
    SimpleSelect,
    SimpleUpdate,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Extending {
    EXTENDING_TypeNameList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ExtendingSimple {
    EXTENDING_SimpleTypeNameList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ExtensionPackageStmt {
    CreateExtensionPackageMigrationStmt,
    CreateExtensionPackageStmt,
    DropExtensionPackageMigrationStmt,
    DropExtensionPackageStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ExtensionRequirementDeclaration {
    USING_EXTENSION_ShortNodeName_OptExtensionVersion,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ExtensionStmt {
    AlterExtensionStmt,
    CreateExtensionStmt,
    DropExtensionStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ExtensionVersion {
    VERSION_BaseStringConstant,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FilterClause {
    FILTER_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FreeShape {
    LBRACE_FreeComputableShapePointerList_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FreeSimpleShapePointer {
    FreeStepName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FreeStepName {
    DUNDERTYPE,
    ShortNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FromFunction {
    USING_Identifier_BaseStringConstant,
    USING_Identifier_EXPRESSION,
    USING_Identifier_FUNCTION_BaseStringConstant,
    USING_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FullTypeExpr {
    FullTypeExpr_AMPER_FullTypeExpr,
    FullTypeExpr_PIPE_FullTypeExpr,
    LPAREN_FullTypeExpr_RPAREN,
    TYPEOF_Expr,
    TypeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FuncApplication {
    NodeName_LPAREN_OptFuncArgList_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=FuncArgList, separator=COMMA, trailing=true)]
#[stub()]
pub enum FuncCallArg {
    FuncCallArgExpr_OptFilterClause_OptSortClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FuncCallArgExpr {
    AnyIdentifier_ASSIGN_Expr,
    Expr,
    PARAMETER_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum FuncDeclArg {
    OptParameterKind_FuncDeclArgName_OptDefault,
    OptParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FuncDeclArgName {
    Identifier,
    PARAMETER,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FuncDeclArgs {
    FuncDeclArgList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FuncExpr {
    FuncApplication,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FunctionDeclaration {
    FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FunctionDeclarationShort {
    FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSingleSDLCommandBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FunctionType {
    FullTypeExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FutureRequirementDeclaration {
    USING_FUTURE_ShortNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum FutureStmt {
    CreateFutureStmt,
    DropFutureStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum GlobalDeclaration {
    GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
    PtrQuals_GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum GlobalDeclarationShort {
    GLOBAL_NodeName_ASSIGN_Expr,
    PtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
    GLOBAL_NodeName_PtrTarget,
    PtrQuals_GLOBAL_NodeName_PtrTarget,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::GroupingAtom)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum GroupingAtom {
    GroupingIdent,
    LPAREN_GroupingIdentList_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::GroupingElement)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum GroupingElement {
    CUBE_LPAREN_GroupingAtomList_RPAREN,
    GroupingAtom,
    LBRACE_GroupingElementList_RBRACE,
    ROLLUP_LPAREN_GroupingAtomList_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::GroupingAtom)]
#[list(separator=COMMA)]
#[stub()]
pub enum GroupingIdent {
    AT_Identifier,
    DOT_Identifier,
    Identifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(String)]
#[stub()]
pub enum Identifier {
    IDENT,
    UnreservedKeyword,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum IfThenElseExpr {
    IF_Expr_THEN_Expr_ELSE_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum IndexArg {
    AnyIdentifier_ASSIGN_Expr,
    FuncDeclArgName_OptDefault,
    FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
    ParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum IndexDeclaration {
    ABSTRACT_INDEX_NodeName_OptExtendingSimple_CreateIndexSDLCommandsBlock,
    ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_CreateIndexSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum IndexDeclarationShort {
    ABSTRACT_INDEX_NodeName_OptExtendingSimple,
    ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum IndexExtArgList {
    LPAREN_OptIndexArgList_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum IndirectionEl {
    LBRACKET_COLON_Expr_RBRACKET,
    LBRACKET_Expr_COLON_Expr_RBRACKET,
    LBRACKET_Expr_COLON_RBRACKET,
    LBRACKET_Expr_RBRACKET,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::InternalGroupQuery)]
#[stub()]
pub enum InternalGroup {
    FOR_GROUP_OptionallyAliasedExpr_UsingClause_ByClause_IN_Identifier_OptGroupingAlias_UNION_OptionallyAliasedExpr_OptFilterClause_OptSortClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum LimitClause {
    LIMIT_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum LinkDeclaration {
    ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_CreateLinkSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum LinkDeclarationShort {
    ABSTRACT_LINK_PtrNodeName_OptExtendingSimple,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ModuleDeclaration {
    MODULE_ModuleName_SDLCommandBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ModuleName {
    DotName,
    ModuleName_DOUBLECOLON_DotName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum NamedTuple {
    LPAREN_NamedTupleElementList_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum NamedTupleElement {
    ShortNodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum NestedQLBlockStmt {
    OptWithDDLStmt,
    SetFieldStmt,
    Stmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum NodeName {
    BaseName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum NontrivialTypeExpr {
    LPAREN_FullTypeExpr_RPAREN,
    TYPEOF_Expr,
    TypeExpr_AMPER_TypeExpr,
    TypeExpr_PIPE_TypeExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ObjectTypeDeclaration {
    ABSTRACT_TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
    TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ObjectTypeDeclarationShort {
    ABSTRACT_TYPE_NodeName_OptExtendingSimple,
    TYPE_NodeName_OptExtendingSimple,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OffsetClause {
    OFFSET_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OnExpr {
    ON_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OnSourceDeleteResetStmt {
    RESET_ON_SOURCE_DELETE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OnSourceDeleteStmt {
    ON_SOURCE_DELETE_ALLOW,
    ON_SOURCE_DELETE_DELETE_TARGET,
    ON_SOURCE_DELETE_DELETE_TARGET_IF_ORPHAN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OnTargetDeleteResetStmt {
    RESET_ON_TARGET_DELETE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OnTargetDeleteStmt {
    ON_TARGET_DELETE_ALLOW,
    ON_TARGET_DELETE_DEFERRED_RESTRICT,
    ON_TARGET_DELETE_DELETE_SOURCE,
    ON_TARGET_DELETE_RESTRICT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OperatorCode {
    USING_Identifier_BaseStringConstant,
    USING_Identifier_EXPRESSION,
    USING_Identifier_FUNCTION_BaseStringConstant,
    USING_Identifier_OPERATOR_BaseStringConstant,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OperatorKind {
    INFIX,
    POSTFIX,
    PREFIX,
    TERNARY,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptAlterUsingClause {
    USING_ParenExpr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptAnySubShape {
    COLON_Shape,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptConcreteConstraintArgList {
    LPAREN_OptPosCallArgList_RPAREN,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateAccessPolicyCommandsBlock {
    CreateAccessPolicyCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateAnnotationCommandsBlock {
    CreateAnnotationCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateCommandsBlock {
    CreateCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateConcreteLinkCommandsBlock {
    CreateConcreteLinkCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateConcretePropertyCommandsBlock {
    CreateConcretePropertyCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateDatabaseCommandsBlock {
    CreateDatabaseCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateExtensionCommandsBlock {
    CreateExtensionCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateExtensionPackageCommandsBlock {
    CreateExtensionPackageCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateGlobalCommandsBlock {
    CreateGlobalCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateIndexCommandsBlock {
    CreateIndexCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateIndexMatchCommandsBlock {
    CreateIndexMatchCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateLinkCommandsBlock {
    CreateLinkCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateMigrationCommandsBlock {
    CreateMigrationCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateObjectTypeCommandsBlock {
    CreateObjectTypeCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateOperatorCommandsBlock {
    CreateOperatorCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreatePropertyCommandsBlock {
    CreatePropertyCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreatePseudoTypeCommandsBlock {
    CreatePseudoTypeCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateRewriteCommandsBlock {
    CreateRewriteCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateRoleCommandsBlock {
    CreateRoleCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateScalarTypeCommandsBlock {
    CreateScalarTypeCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptCreateTriggerCommandsBlock {
    CreateTriggerCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDefault {
    EQUALS_Expr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDeferred {
    DEFERRED,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDelegated {
    DELEGATED,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDirection {
    ASC,
    DESC,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDropConcreteIndexCommandsBlock {
    DropConcreteIndexCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDropConcreteLinkCommandsBlock {
    DropConcreteLinkCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDropLinkCommandsBlock {
    DropLinkCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptDropObjectTypeCommandsBlock {
    DropObjectTypeCommandsBlock,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptExceptExpr {
    EXCEPT_ParenExpr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptExprList {
    ExprList,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptExtending {
    Extending,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptExtendingSimple {
    ExtendingSimple,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptExtensionVersion {
    ExtensionVersion,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptFilterClause {
    FilterClause,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptFuncArgList {
    FuncArgList,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(Option::<ast::GroupQuery>)]
#[stub()]
pub enum OptGroupingAlias {
    COMMA_Identifier,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptIfNotExists {
    IF_NOT_EXISTS,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptIndexArgList {
    IndexArgList,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptIndexExtArgList {
    IndexExtArgList,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptMigrationNameParentName {
    ShortNodeName,
    ShortNodeName_ONTO_ShortNodeName,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptNonesOrder {
    EMPTY_FIRST,
    EMPTY_LAST,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptOnExpr {
    OnExpr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptParameterKind {
    ParameterKind,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptPosCallArgList {
    PosCallArgList,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptPosition {
    AFTER_NodeName,
    BEFORE_NodeName,
    FIRST,
    LAST,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptPtrQuals {
    PtrQuals,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptPtrTarget {
    PtrTarget,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptSelectLimit {
    SelectLimit,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptSemicolons {
    Semicolons,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptShortExtending {
    ShortExtending,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptSortClause {
    SortClause,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptSuperuser {
    SUPERUSER,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptTransactionModeList {
    TransactionModeList,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptTypeIntersection {
    TypeIntersection,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptTypeQualifier {
    OPTIONAL,
    SET_OF,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptUnlessConflictClause {
    UnlessConflictCause,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptUsingBlock {
    USING_ParenExpr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(Vec::<ast::AliasedExpr>)]
#[stub()]
pub enum OptUsingClause {
    UsingClause,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptWhenBlock {
    WHEN_ParenExpr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptWithDDLStmt {
    DDLWithBlock_WithDDLStmt,
    WithDDLStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(bool)]
#[stub()]
pub enum OptionalOptional {
    OPTIONAL,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum OptionallyAliasedExpr {
    AliasedExpr,
    Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(name=OrderbyList, separator=THEN)]
#[stub()]
pub enum OrderbyExpr {
    Expr_OptDirection_OptNonesOrder,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ParameterKind {
    NAMEDONLY,
    VARIADIC,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ParenExpr {
    LPAREN_ExprStmt_RPAREN,
    LPAREN_Expr_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ParenTypeExpr {
    LPAREN_FullTypeExpr_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PartialReservedKeyword {
    EXCEPT,
    INTERSECT,
    UNION,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Path {
    Expr_PathStep_P_DOT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PathNodeName {
    PtrIdentifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PathStep {
    AT_PathNodeName,
    DOTBW_PathStepName,
    DOT_ICONST,
    DOT_PathStepName,
    TypeIntersection,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PathStepName {
    DUNDERTYPE,
    PathNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PointerName {
    DUNDERTYPE,
    PtrNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PopulateMigrationStmt {
    POPULATE_MIGRATION,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum PosCallArg {
    Expr_OptFilterClause_OptSortClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PropertyDeclaration {
    ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_CreatePropertySDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PropertyDeclarationShort {
    ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PtrIdentifier {
    Identifier,
    PartialReservedKeyword,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PtrName {
    PtrIdentifier,
    QualifiedName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PtrNodeName {
    PtrName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PtrQualifiedNodeName {
    QualifiedName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum PtrTarget {
    ARROW_FullTypeExpr,
    COLON_FullTypeExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum QualifiedName {
    DUNDERSTD_DOUBLECOLON_ColonedIdents,
    Identifier_DOUBLECOLON_ColonedIdents,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum RenameStmt {
    RENAME_TO_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ResetFieldStmt {
    RESET_DEFAULT,
    RESET_IDENT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ResetSchemaStmt {
    RESET_SCHEMA_TO_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::SessionResetAliasDecl)]
#[stub()]
pub enum ResetStmt {
    RESET_ALIAS_Identifier,
    RESET_ALIAS_STAR,
    RESET_MODULE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum RewriteDeclarationBlock {
    REWRITE_RewriteKindList_USING_ParenExpr_CreateRewriteSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum RewriteDeclarationShort {
    REWRITE_RewriteKindList_USING_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum RewriteKind {
    INSERT,
    UPDATE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum RoleStmt {
    AlterRoleStmt,
    CreateRoleStmt,
    DropRoleStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SDLCommandBlock {
    LBRACE_OptSemicolons_RBRACE,
    LBRACE_OptSemicolons_SDLStatements_RBRACE,
    LBRACE_OptSemicolons_SDLShortStatement_RBRACE,
    LBRACE_OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement_RBRACE,
    LBRACE_OptSemicolons_SDLStatements_Semicolons_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SDLDocument {
    OptSemicolons,
    OptSemicolons_SDLStatements,
    OptSemicolons_SDLStatements_Semicolons,
    OptSemicolons_SDLShortStatement,
    OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SDLStatement {
    SDLBlockStatement,
    SDLShortStatement_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SDLStatements {
    SDLStatement,
    SDLStatements_OptSemicolons_SDLStatement,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ScalarTypeDeclaration {
    ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
    SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ScalarTypeDeclarationShort {
    ABSTRACT_SCALAR_TYPE_NodeName_OptExtending,
    SCALAR_TYPE_NodeName_OptExtending,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SchemaItem {
    SchemaObjectClass_NodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SelectLimit {
    LimitClause,
    OffsetClause,
    OffsetClause_LimitClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Semicolons {
    SEMICOLON,
    Semicolons_SEMICOLON,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::Command)]
#[stub()]
pub enum SessionStmt {
    ResetStmt,
    SetStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Set {
    LBRACE_OptExprList_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetAnnotation {
    ANNOTATION_NodeName_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetCardinalityStmt {
    RESET_CARDINALITY_OptAlterUsingClause,
    SET_MULTI,
    SET_SINGLE_OptAlterUsingClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetDelegatedStmt {
    RESET_DELEGATED,
    SET_DELEGATED,
    SET_NOT_DELEGATED,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetField {
    Identifier_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetFieldStmt {
    SET_Identifier_ASSIGN_Expr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetGlobalTypeStmt {
    RESET_TYPE,
    SETTYPE_FullTypeExpr_OptAlterUsingClause,
    SETTYPE_FullTypeExpr_RESET_TO_DEFAULT,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetPointerTypeStmt {
    RESET_TYPE,
    SETTYPE_FullTypeExpr_OptAlterUsingClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetRequiredInCreateStmt {
    SET_REQUIRED_OptAlterUsingClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SetRequiredStmt {
    DROP_REQUIRED,
    RESET_OPTIONALITY,
    SET_OPTIONAL,
    SET_REQUIRED_OptAlterUsingClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::SessionSetAliasDecl)]
#[stub()]
pub enum SetStmt {
    SET_ALIAS_Identifier_AS_MODULE_ModuleName,
    SET_MODULE_ModuleName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Shape {
    LBRACE_RBRACE,
    LBRACE_ShapeElementList_RBRACE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum ShapeElement {
    ComputableShapePointer,
    ShapePointer_OptAnySubShape_OptFilterClause_OptSortClause_OptSelectLimit,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ShapePath {
    AT_PathNodeName,
    PathStepName_OptTypeIntersection,
    Splat,
    TypeIntersection_DOT_PathStepName_OptTypeIntersection,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ShapePointer {
    ShapePath,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum ShortExtending {
    EXTENDING_ShortNodeNameList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum ShortNodeName {
    Identifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::DeleteQuery)]
#[stub()]
pub enum SimpleDelete {
    DELETE_Expr_OptFilterClause_OptSortClause_OptSelectLimit,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::ForQuery)]
#[stub()]
pub enum SimpleFor {
    FOR_OptionalOptional_Identifier_IN_AtomicExpr_UNION_Expr,
    FOR_OptionalOptional_Identifier_IN_AtomicExpr_ExprStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::GroupQuery)]
#[stub()]
pub enum SimpleGroup {
    GROUP_OptionallyAliasedExpr_OptUsingClause_ByClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::InsertQuery)]
#[stub()]
pub enum SimpleInsert {
    INSERT_Expr_OptUnlessConflictClause,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::SelectQuery)]
#[stub()]
pub enum SimpleSelect {
    SELECT_OptionallyAliasedExpr_OptFilterClause_OptSortClause_OptSelectLimit,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SimpleShapePath {
    AT_PathNodeName,
    PathStepName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SimpleShapePointer {
    SimpleShapePath,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum SimpleTypeName {
    ANYOBJECT,
    ANYTUPLE,
    ANYTYPE,
    PtrNodeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::UpdateQuery)]
#[stub()]
pub enum SimpleUpdate {
    UPDATE_Expr_OptFilterClause_SET_Shape,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::Command)]
#[stub()]
pub enum SingleStatement {
    ConfigStmt,
    DDLStmt,
    IfThenElseExpr,
    SessionStmt,
    Stmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum SortClause {
    ORDERBY_OrderbyList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum StartMigrationStmt {
    START_MIGRATION_TO_SDLCommandBlock,
    START_MIGRATION_REWRITE,
    START_MIGRATION_TO_COMMITTED_SCHEMA,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum StatementBlock {
    SingleStatement,
    StatementBlock_Semicolons_SingleStatement,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(ast::Command)]
#[stub()]
pub enum Stmt {
    AdministerStmt,
    AnalyzeStmt,
    DescribeStmt,
    ExprStmt,
    TransactionStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum Subtype {
    BaseNumberConstant,
    BaseStringConstant,
    FullTypeExpr,
    Identifier_COLON_FullTypeExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum TransactionMode {
    DEFERRABLE,
    ISOLATION_SERIALIZABLE,
    NOT_DEFERRABLE,
    READ_ONLY,
    READ_WRITE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TransactionStmt {
    COMMIT,
    DECLARE_SAVEPOINT_Identifier,
    RELEASE_SAVEPOINT_Identifier,
    ROLLBACK,
    ROLLBACK_TO_SAVEPOINT_Identifier,
    START_TRANSACTION_OptTransactionModeList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TriggerDeclarationBlock {
    TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_CreateTriggerSDLCommandsBlock,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TriggerDeclarationShort {
    TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum TriggerKind {
    DELETE,
    INSERT,
    UPDATE,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TriggerScope {
    ALL,
    EACH,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TriggerTiming {
    AFTER,
    AFTER_COMMIT_OF,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Tuple {
    LPAREN_Expr_COMMA_OptExprList_RPAREN,
    LPAREN_RPAREN,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TypeExpr {
    NontrivialTypeExpr,
    SimpleTypeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum TypeIntersection {
    LBRACKET_IS_FullTypeExpr_RBRACKET,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA)]
#[stub()]
pub enum TypeName {
    CollectionTypeName,
    SimpleTypeName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum UnlessConflictCause {
    UNLESS_CONFLICT_UnlessConflictSpecifier,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum UnlessConflictSpecifier {
    ON_Expr,
    ON_Expr_ELSE_Expr,
    epsilon,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum UnqualifiedPointerName {
    PointerName,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
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

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum Using {
    USING_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(Vec::<ast::AliasedExpr>)]
#[stub()]
pub enum UsingClause {
    USING_AliasedExprList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum UsingStmt {
    RESET_EXPRESSION,
    USING_ParenExpr,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum WithBlock {
    WITH_WithDeclList,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[stub()]
pub enum WithDDLStmt {
    InnerDDLStmt,
}

#[derive(edgeql_parser_derive::Reduce)]
#[output(TodoAst)]
#[list(separator=COMMA, trailing=true)]
#[stub()]
pub enum WithDecl {
    AliasDecl,
}
