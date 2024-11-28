// DO NOT EDIT. This file was generated with:
//
// $ edb gen-rust-ast

impl super::FromId for super::AbortMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            0 => Self::ABORT_MIGRATION,
            1 => Self::ABORT_MIGRATION_REWRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessKind {
    fn from_id(id: usize) -> Self {
        match id {
            2 => Self::ALL,
            3 => Self::DELETE,
            4 => Self::INSERT,
            5 => Self::SELECT,
            6 => Self::UPDATE,
            7 => Self::UPDATE_READ,
            8 => Self::UPDATE_WRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessKindList {
    fn from_id(id: usize) -> Self {
        match id {
            9 => Self::AccessKind,
            10 => Self::AccessKindList_COMMA_AccessKind,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessPermStmt {
    fn from_id(id: usize) -> Self {
        match id {
            11 => Self::AccessPolicyAction_AccessKindList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessPolicyAction {
    fn from_id(id: usize) -> Self {
        match id {
            12 => Self::ALLOW,
            13 => Self::DENY,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessPolicyDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            14 => Self::ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_CreateAccessPolicySDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessPolicyDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            15 => Self::ACCESS_POLICY_ShortNodeName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessUsingStmt {
    fn from_id(id: usize) -> Self {
        match id {
            16 => Self::RESET_EXPRESSION,
            17 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AccessWhenStmt {
    fn from_id(id: usize) -> Self {
        match id {
            18 => Self::RESET_WHEN,
            19 => Self::WHEN_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AdministerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            20 => Self::ADMINISTER_FuncExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AliasDecl {
    fn from_id(id: usize) -> Self {
        match id {
            21 => Self::AliasedExpr,
            22 => Self::Identifier_AS_MODULE_ModuleName,
            23 => Self::MODULE_ModuleName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AliasDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            24 => Self::ALIAS_NodeName_CreateAliasSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AliasDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            25 => Self::ALIAS_NodeName_CreateAliasSingleSDLCommandBlock,
            26 => Self::ALIAS_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AliasedExpr {
    fn from_id(id: usize) -> Self {
        match id {
            27 => Self::Identifier_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AliasedExprList {
    fn from_id(id: usize) -> Self {
        match id {
            28 => Self::AliasedExprListInner,
            29 => Self::AliasedExprListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AliasedExprListInner {
    fn from_id(id: usize) -> Self {
        match id {
            30 => Self::AliasedExpr,
            31 => Self::AliasedExprListInner_COMMA_AliasedExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAbstract {
    fn from_id(id: usize) -> Self {
        match id {
            32 => Self::DROP_ABSTRACT,
            33 => Self::RESET_ABSTRACT,
            34 => Self::SET_ABSTRACT,
            35 => Self::SET_NOT_ABSTRACT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAccessPolicyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            36 => Self::AccessPermStmt,
            37 => Self::AccessUsingStmt,
            38 => Self::AccessWhenStmt,
            39 => Self::AlterAnnotationValueStmt,
            40 => Self::CreateAnnotationValueStmt,
            41 => Self::DropAnnotationValueStmt,
            42 => Self::RenameStmt,
            43 => Self::ResetFieldStmt,
            44 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAccessPolicyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            45 => Self::AlterAccessPolicyCommand,
            46 => Self::LBRACE_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE,
            47 => Self::LBRACE_OptSemicolons_RBRACE,
            48 => Self::LBRACE_Semicolons_AlterAccessPolicyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAccessPolicyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            49 => Self::AlterAccessPolicyCommand,
            50 => Self::AlterAccessPolicyCommandsList_Semicolons_AlterAccessPolicyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAccessPolicyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            51 => Self::ALTER_ACCESS_POLICY_UnqualifiedPointerName_AlterAccessPolicyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAliasCommand {
    fn from_id(id: usize) -> Self {
        match id {
            52 => Self::AlterAnnotationValueStmt,
            53 => Self::CreateAnnotationValueStmt,
            54 => Self::DropAnnotationValueStmt,
            55 => Self::RenameStmt,
            56 => Self::ResetFieldStmt,
            57 => Self::SetFieldStmt,
            58 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAliasCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            59 => Self::AlterAliasCommand,
            60 => Self::LBRACE_AlterAliasCommandsList_OptSemicolons_RBRACE,
            61 => Self::LBRACE_OptSemicolons_RBRACE,
            62 => Self::LBRACE_Semicolons_AlterAliasCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAliasCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            63 => Self::AlterAliasCommand,
            64 => Self::AlterAliasCommandsList_Semicolons_AlterAliasCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAliasStmt {
    fn from_id(id: usize) -> Self {
        match id {
            65 => Self::ALTER_ALIAS_NodeName_AlterAliasCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAnnotationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            66 => Self::AlterAnnotationValueStmt,
            67 => Self::CreateAnnotationValueStmt,
            68 => Self::DropAnnotationValueStmt,
            69 => Self::RenameStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAnnotationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            70 => Self::AlterAnnotationCommand,
            71 => Self::LBRACE_AlterAnnotationCommandsList_OptSemicolons_RBRACE,
            72 => Self::LBRACE_OptSemicolons_RBRACE,
            73 => Self::LBRACE_Semicolons_AlterAnnotationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAnnotationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            74 => Self::AlterAnnotationCommand,
            75 => Self::AlterAnnotationCommandsList_Semicolons_AlterAnnotationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAnnotationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            76 => Self::ALTER_ABSTRACT_ANNOTATION_NodeName_AlterAnnotationCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterAnnotationValueStmt {
    fn from_id(id: usize) -> Self {
        match id {
            77 => Self::ALTER_ANNOTATION_NodeName_ASSIGN_Expr,
            78 => Self::ALTER_ANNOTATION_NodeName_DROP_OWNED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterBranchCommand {
    fn from_id(id: usize) -> Self {
        match id {
            79 => Self::RenameStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterBranchCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            80 => Self::AlterBranchCommand,
            81 => Self::LBRACE_AlterBranchCommandsList_OptSemicolons_RBRACE,
            82 => Self::LBRACE_OptSemicolons_RBRACE,
            83 => Self::LBRACE_Semicolons_AlterBranchCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterBranchCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            84 => Self::AlterBranchCommand,
            85 => Self::AlterBranchCommandsList_Semicolons_AlterBranchCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterBranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            86 => Self::ALTER_BRANCH_DatabaseName_BranchOptions_AlterBranchCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCastCommand {
    fn from_id(id: usize) -> Self {
        match id {
            87 => Self::AlterAnnotationValueStmt,
            88 => Self::CreateAnnotationValueStmt,
            89 => Self::DropAnnotationValueStmt,
            90 => Self::ResetFieldStmt,
            91 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCastCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            92 => Self::AlterCastCommand,
            93 => Self::LBRACE_AlterCastCommandsList_OptSemicolons_RBRACE,
            94 => Self::LBRACE_OptSemicolons_RBRACE,
            95 => Self::LBRACE_Semicolons_AlterCastCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCastCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            96 => Self::AlterCastCommand,
            97 => Self::AlterCastCommandsList_Semicolons_AlterCastCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCastStmt {
    fn from_id(id: usize) -> Self {
        match id {
            98 => Self::ALTER_CAST_FROM_TypeName_TO_TypeName_AlterCastCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCommand {
    fn from_id(id: usize) -> Self {
        match id {
            99 => Self::AlterAnnotationValueStmt,
            100 => Self::CreateAnnotationValueStmt,
            101 => Self::DropAnnotationValueStmt,
            102 => Self::RenameStmt,
            103 => Self::ResetFieldStmt,
            104 => Self::SetFieldStmt,
            105 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            106 => Self::AlterCommand,
            107 => Self::LBRACE_AlterCommandsList_OptSemicolons_RBRACE,
            108 => Self::LBRACE_OptSemicolons_RBRACE,
            109 => Self::LBRACE_Semicolons_AlterCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            110 => Self::AlterCommand,
            111 => Self::AlterCommandsList_Semicolons_AlterCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteConstraintCommand {
    fn from_id(id: usize) -> Self {
        match id {
            112 => Self::AlterAbstract,
            113 => Self::AlterAnnotationValueStmt,
            114 => Self::AlterOwnedStmt,
            115 => Self::CreateAnnotationValueStmt,
            116 => Self::DropAnnotationValueStmt,
            117 => Self::ResetFieldStmt,
            118 => Self::SetDelegatedStmt,
            119 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteConstraintCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            120 => Self::AlterConcreteConstraintCommand,
            121 => Self::LBRACE_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE,
            122 => Self::LBRACE_OptSemicolons_RBRACE,
            123 => Self::LBRACE_Semicolons_AlterConcreteConstraintCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteConstraintCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            124 => Self::AlterConcreteConstraintCommand,
            125 => Self::AlterConcreteConstraintCommandsList_Semicolons_AlterConcreteConstraintCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            126 => Self::ALTER_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_AlterConcreteConstraintCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            127 => Self::AlterAnnotationValueStmt,
            128 => Self::AlterDeferredStmt,
            129 => Self::AlterOwnedStmt,
            130 => Self::CreateAnnotationValueStmt,
            131 => Self::DropAnnotationValueStmt,
            132 => Self::ResetFieldStmt,
            133 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            134 => Self::AlterConcreteIndexCommand,
            135 => Self::LBRACE_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE,
            136 => Self::LBRACE_OptSemicolons_RBRACE,
            137 => Self::LBRACE_Semicolons_AlterConcreteIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            138 => Self::AlterConcreteIndexCommand,
            139 => Self::AlterConcreteIndexCommandsList_Semicolons_AlterConcreteIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            140 => Self::ALTER_INDEX_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock,
            141 => Self::ALTER_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_AlterConcreteIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            142 => Self::AlterAnnotationValueStmt,
            143 => Self::AlterConcreteConstraintStmt,
            144 => Self::AlterConcreteIndexStmt,
            145 => Self::AlterConcretePropertyStmt,
            146 => Self::AlterOwnedStmt,
            147 => Self::AlterRewriteStmt,
            148 => Self::AlterSimpleExtending,
            149 => Self::CreateAnnotationValueStmt,
            150 => Self::CreateConcreteConstraintStmt,
            151 => Self::CreateConcreteIndexStmt,
            152 => Self::CreateConcretePropertyStmt,
            153 => Self::CreateRewriteStmt,
            154 => Self::DropAnnotationValueStmt,
            155 => Self::DropConcreteConstraintStmt,
            156 => Self::DropConcreteIndexStmt,
            157 => Self::DropConcretePropertyStmt,
            158 => Self::DropRewriteStmt,
            159 => Self::OnSourceDeleteResetStmt,
            160 => Self::OnSourceDeleteStmt,
            161 => Self::OnTargetDeleteResetStmt,
            162 => Self::OnTargetDeleteStmt,
            163 => Self::RenameStmt,
            164 => Self::ResetFieldStmt,
            165 => Self::SetCardinalityStmt,
            166 => Self::SetFieldStmt,
            167 => Self::SetPointerTypeStmt,
            168 => Self::SetRequiredStmt,
            169 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            170 => Self::AlterConcreteLinkCommand,
            171 => Self::LBRACE_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE,
            172 => Self::LBRACE_OptSemicolons_RBRACE,
            173 => Self::LBRACE_Semicolons_AlterConcreteLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            174 => Self::AlterConcreteLinkCommand,
            175 => Self::AlterConcreteLinkCommandsList_Semicolons_AlterConcreteLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcreteLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            176 => Self::ALTER_LINK_UnqualifiedPointerName_AlterConcreteLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcretePropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            177 => Self::AlterAnnotationValueStmt,
            178 => Self::AlterConcreteConstraintStmt,
            179 => Self::AlterOwnedStmt,
            180 => Self::AlterRewriteStmt,
            181 => Self::AlterSimpleExtending,
            182 => Self::CreateAnnotationValueStmt,
            183 => Self::CreateConcreteConstraintStmt,
            184 => Self::CreateRewriteStmt,
            185 => Self::DropAnnotationValueStmt,
            186 => Self::DropConcreteConstraintStmt,
            187 => Self::DropRewriteStmt,
            188 => Self::RenameStmt,
            189 => Self::ResetFieldStmt,
            190 => Self::SetCardinalityStmt,
            191 => Self::SetFieldStmt,
            192 => Self::SetPointerTypeStmt,
            193 => Self::SetRequiredStmt,
            194 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcretePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            195 => Self::AlterConcretePropertyCommand,
            196 => Self::LBRACE_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE,
            197 => Self::LBRACE_OptSemicolons_RBRACE,
            198 => Self::LBRACE_Semicolons_AlterConcretePropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcretePropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            199 => Self::AlterConcretePropertyCommand,
            200 => Self::AlterConcretePropertyCommandsList_Semicolons_AlterConcretePropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConcretePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            201 => Self::ALTER_PROPERTY_UnqualifiedPointerName_AlterConcretePropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            202 => Self::ALTER_ABSTRACT_CONSTRAINT_NodeName_AlterCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterCurrentMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            203 => Self::ALTER_CURRENT_MIGRATION_REJECT_PROPOSED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterDatabaseCommand {
    fn from_id(id: usize) -> Self {
        match id {
            204 => Self::RenameStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterDatabaseCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            205 => Self::AlterDatabaseCommand,
            206 => Self::LBRACE_AlterDatabaseCommandsList_OptSemicolons_RBRACE,
            207 => Self::LBRACE_OptSemicolons_RBRACE,
            208 => Self::LBRACE_Semicolons_AlterDatabaseCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterDatabaseCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            209 => Self::AlterDatabaseCommand,
            210 => Self::AlterDatabaseCommandsList_Semicolons_AlterDatabaseCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterDatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            211 => Self::ALTER_DATABASE_DatabaseName_AlterDatabaseCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterDeferredStmt {
    fn from_id(id: usize) -> Self {
        match id {
            212 => Self::DROP_DEFERRED,
            213 => Self::SET_DEFERRED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterExtending {
    fn from_id(id: usize) -> Self {
        match id {
            214 => Self::AlterAbstract,
            215 => Self::DROP_EXTENDING_TypeNameList,
            216 => Self::EXTENDING_TypeNameList_OptPosition,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            217 => Self::ALTER_EXTENSION_ShortNodeName_TO_ExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionCommand {
    fn from_id(id: usize) -> Self {
        match id {
            218 => Self::AlterAnnotationValueStmt,
            219 => Self::CreateAnnotationValueStmt,
            220 => Self::DropAnnotationValueStmt,
            221 => Self::FromFunction,
            222 => Self::RenameStmt,
            223 => Self::ResetFieldStmt,
            224 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            225 => Self::AlterFunctionCommand,
            226 => Self::LBRACE_AlterFunctionCommandsList_OptSemicolons_RBRACE,
            227 => Self::LBRACE_OptSemicolons_RBRACE,
            228 => Self::LBRACE_Semicolons_AlterFunctionCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            229 => Self::AlterFunctionCommand,
            230 => Self::AlterFunctionCommandsList_Semicolons_AlterFunctionCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            231 => Self::ALTER_FUNCTION_NodeName_CreateFunctionArgs_AlterFunctionCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalCommand {
    fn from_id(id: usize) -> Self {
        match id {
            232 => Self::AlterAnnotationValueStmt,
            233 => Self::CreateAnnotationValueStmt,
            234 => Self::DropAnnotationValueStmt,
            235 => Self::RenameStmt,
            236 => Self::ResetFieldStmt,
            237 => Self::SetCardinalityStmt,
            238 => Self::SetFieldStmt,
            239 => Self::SetGlobalTypeStmt,
            240 => Self::SetRequiredStmt,
            241 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            242 => Self::AlterGlobalCommand,
            243 => Self::LBRACE_AlterGlobalCommandsList_OptSemicolons_RBRACE,
            244 => Self::LBRACE_OptSemicolons_RBRACE,
            245 => Self::LBRACE_Semicolons_AlterGlobalCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            246 => Self::AlterGlobalCommand,
            247 => Self::AlterGlobalCommandsList_Semicolons_AlterGlobalCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalStmt {
    fn from_id(id: usize) -> Self {
        match id {
            248 => Self::ALTER_GLOBAL_NodeName_AlterGlobalCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            249 => Self::AlterAnnotationValueStmt,
            250 => Self::CreateAnnotationValueStmt,
            251 => Self::DropAnnotationValueStmt,
            252 => Self::RenameStmt,
            253 => Self::ResetFieldStmt,
            254 => Self::SetFieldStmt,
            255 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            256 => Self::AlterIndexCommand,
            257 => Self::LBRACE_AlterIndexCommandsList_OptSemicolons_RBRACE,
            258 => Self::LBRACE_OptSemicolons_RBRACE,
            259 => Self::LBRACE_Semicolons_AlterIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            260 => Self::AlterIndexCommand,
            261 => Self::AlterIndexCommandsList_Semicolons_AlterIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            262 => Self::ALTER_ABSTRACT_INDEX_NodeName_AlterIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            263 => Self::AlterAnnotationValueStmt,
            264 => Self::AlterConcreteConstraintStmt,
            265 => Self::AlterConcreteIndexStmt,
            266 => Self::AlterConcretePropertyStmt,
            267 => Self::AlterRewriteStmt,
            268 => Self::AlterSimpleExtending,
            269 => Self::CreateAnnotationValueStmt,
            270 => Self::CreateConcreteConstraintStmt,
            271 => Self::CreateConcreteIndexStmt,
            272 => Self::CreateConcretePropertyStmt,
            273 => Self::CreateRewriteStmt,
            274 => Self::DropAnnotationValueStmt,
            275 => Self::DropConcreteConstraintStmt,
            276 => Self::DropConcreteIndexStmt,
            277 => Self::DropConcretePropertyStmt,
            278 => Self::DropRewriteStmt,
            279 => Self::RenameStmt,
            280 => Self::ResetFieldStmt,
            281 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            282 => Self::AlterLinkCommand,
            283 => Self::LBRACE_AlterLinkCommandsList_OptSemicolons_RBRACE,
            284 => Self::LBRACE_OptSemicolons_RBRACE,
            285 => Self::LBRACE_Semicolons_AlterLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            286 => Self::AlterLinkCommand,
            287 => Self::AlterLinkCommandsList_Semicolons_AlterLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            288 => Self::ALTER_ABSTRACT_LINK_PtrNodeName_AlterLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            289 => Self::ResetFieldStmt,
            290 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            291 => Self::AlterMigrationCommand,
            292 => Self::LBRACE_AlterMigrationCommandsList_OptSemicolons_RBRACE,
            293 => Self::LBRACE_OptSemicolons_RBRACE,
            294 => Self::LBRACE_Semicolons_AlterMigrationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            295 => Self::AlterMigrationCommand,
            296 => Self::AlterMigrationCommandsList_Semicolons_AlterMigrationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            297 => Self::ALTER_MIGRATION_NodeName_AlterMigrationCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterModuleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            298 => Self::ALTER_MODULE_ModuleName_AlterCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            299 => Self::AlterAccessPolicyStmt,
            300 => Self::AlterAnnotationValueStmt,
            301 => Self::AlterConcreteConstraintStmt,
            302 => Self::AlterConcreteIndexStmt,
            303 => Self::AlterConcreteLinkStmt,
            304 => Self::AlterConcretePropertyStmt,
            305 => Self::AlterSimpleExtending,
            306 => Self::AlterTriggerStmt,
            307 => Self::CreateAccessPolicyStmt,
            308 => Self::CreateAnnotationValueStmt,
            309 => Self::CreateConcreteConstraintStmt,
            310 => Self::CreateConcreteIndexStmt,
            311 => Self::CreateConcreteLinkStmt,
            312 => Self::CreateConcretePropertyStmt,
            313 => Self::CreateTriggerStmt,
            314 => Self::DropAccessPolicyStmt,
            315 => Self::DropAnnotationValueStmt,
            316 => Self::DropConcreteConstraintStmt,
            317 => Self::DropConcreteIndexStmt,
            318 => Self::DropConcreteLinkStmt,
            319 => Self::DropConcretePropertyStmt,
            320 => Self::DropTriggerStmt,
            321 => Self::RenameStmt,
            322 => Self::ResetFieldStmt,
            323 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            324 => Self::AlterObjectTypeCommand,
            325 => Self::LBRACE_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
            326 => Self::LBRACE_OptSemicolons_RBRACE,
            327 => Self::LBRACE_Semicolons_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            328 => Self::AlterObjectTypeCommand,
            329 => Self::AlterObjectTypeCommandsList_Semicolons_AlterObjectTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            330 => Self::ALTER_TYPE_NodeName_AlterObjectTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorCommand {
    fn from_id(id: usize) -> Self {
        match id {
            331 => Self::AlterAnnotationValueStmt,
            332 => Self::CreateAnnotationValueStmt,
            333 => Self::DropAnnotationValueStmt,
            334 => Self::ResetFieldStmt,
            335 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            336 => Self::AlterOperatorCommand,
            337 => Self::LBRACE_AlterOperatorCommandsList_OptSemicolons_RBRACE,
            338 => Self::LBRACE_OptSemicolons_RBRACE,
            339 => Self::LBRACE_Semicolons_AlterOperatorCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            340 => Self::AlterOperatorCommand,
            341 => Self::AlterOperatorCommandsList_Semicolons_AlterOperatorCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorStmt {
    fn from_id(id: usize) -> Self {
        match id {
            342 => Self::ALTER_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_AlterOperatorCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOwnedStmt {
    fn from_id(id: usize) -> Self {
        match id {
            343 => Self::DROP_OWNED,
            344 => Self::SET_OWNED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            345 => Self::AlterAnnotationValueStmt,
            346 => Self::AlterRewriteStmt,
            347 => Self::CreateAnnotationValueStmt,
            348 => Self::CreateRewriteStmt,
            349 => Self::DropAnnotationValueStmt,
            350 => Self::DropRewriteStmt,
            351 => Self::RenameStmt,
            352 => Self::ResetFieldStmt,
            353 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            354 => Self::AlterPropertyCommand,
            355 => Self::LBRACE_AlterPropertyCommandsList_OptSemicolons_RBRACE,
            356 => Self::LBRACE_OptSemicolons_RBRACE,
            357 => Self::LBRACE_Semicolons_AlterPropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            358 => Self::AlterPropertyCommand,
            359 => Self::AlterPropertyCommandsList_Semicolons_AlterPropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            360 => Self::ALTER_ABSTRACT_PROPERTY_PtrNodeName_AlterPropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteCommand {
    fn from_id(id: usize) -> Self {
        match id {
            361 => Self::AlterAnnotationValueStmt,
            362 => Self::CreateAnnotationValueStmt,
            363 => Self::DropAnnotationValueStmt,
            364 => Self::ResetFieldStmt,
            365 => Self::SetFieldStmt,
            366 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            367 => Self::AlterRewriteCommand,
            368 => Self::LBRACE_AlterRewriteCommandsList_OptSemicolons_RBRACE,
            369 => Self::LBRACE_OptSemicolons_RBRACE,
            370 => Self::LBRACE_Semicolons_AlterRewriteCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            371 => Self::AlterRewriteCommand,
            372 => Self::AlterRewriteCommandsList_Semicolons_AlterRewriteCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            373 => Self::ALTER_REWRITE_RewriteKindList_AlterRewriteCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleCommand {
    fn from_id(id: usize) -> Self {
        match id {
            374 => Self::AlterRoleExtending,
            375 => Self::RenameStmt,
            376 => Self::ResetFieldStmt,
            377 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            378 => Self::AlterRoleCommand,
            379 => Self::LBRACE_AlterRoleCommandsList_OptSemicolons_RBRACE,
            380 => Self::LBRACE_OptSemicolons_RBRACE,
            381 => Self::LBRACE_Semicolons_AlterRoleCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            382 => Self::AlterRoleCommand,
            383 => Self::AlterRoleCommandsList_Semicolons_AlterRoleCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleExtending {
    fn from_id(id: usize) -> Self {
        match id {
            384 => Self::DROP_EXTENDING_ShortNodeNameList,
            385 => Self::EXTENDING_ShortNodeNameList_OptPosition,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            386 => Self::ALTER_ROLE_ShortNodeName_AlterRoleCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            387 => Self::AlterAnnotationValueStmt,
            388 => Self::AlterConcreteConstraintStmt,
            389 => Self::AlterExtending,
            390 => Self::CreateAnnotationValueStmt,
            391 => Self::CreateConcreteConstraintStmt,
            392 => Self::DropAnnotationValueStmt,
            393 => Self::DropConcreteConstraintStmt,
            394 => Self::RenameStmt,
            395 => Self::ResetFieldStmt,
            396 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            397 => Self::AlterScalarTypeCommand,
            398 => Self::LBRACE_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
            399 => Self::LBRACE_OptSemicolons_RBRACE,
            400 => Self::LBRACE_Semicolons_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            401 => Self::AlterScalarTypeCommand,
            402 => Self::AlterScalarTypeCommandsList_Semicolons_AlterScalarTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            403 => Self::ALTER_SCALAR_TYPE_NodeName_AlterScalarTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterSimpleExtending {
    fn from_id(id: usize) -> Self {
        match id {
            404 => Self::AlterAbstract,
            405 => Self::DROP_EXTENDING_SimpleTypeNameList,
            406 => Self::EXTENDING_SimpleTypeNameList_OptPosition,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerCommand {
    fn from_id(id: usize) -> Self {
        match id {
            407 => Self::AccessWhenStmt,
            408 => Self::AlterAnnotationValueStmt,
            409 => Self::CreateAnnotationValueStmt,
            410 => Self::DropAnnotationValueStmt,
            411 => Self::RenameStmt,
            412 => Self::ResetFieldStmt,
            413 => Self::SetFieldStmt,
            414 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            415 => Self::AlterTriggerCommand,
            416 => Self::LBRACE_AlterTriggerCommandsList_OptSemicolons_RBRACE,
            417 => Self::LBRACE_OptSemicolons_RBRACE,
            418 => Self::LBRACE_Semicolons_AlterTriggerCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            419 => Self::AlterTriggerCommand,
            420 => Self::AlterTriggerCommandsList_Semicolons_AlterTriggerCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            421 => Self::ALTER_TRIGGER_UnqualifiedPointerName_AlterTriggerCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnalyzeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            422 => Self::ANALYZE_ExprStmt,
            423 => Self::ANALYZE_NamedTuple_ExprStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnnotationDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            424 => Self::ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
            425 => Self::ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnnotationDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            426 => Self::ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple,
            427 => Self::ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnyIdentifier {
    fn from_id(id: usize) -> Self {
        match id {
            428 => Self::PtrIdentifier,
            429 => Self::ReservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnyNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            430 => Self::AnyIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AtomicExpr {
    fn from_id(id: usize) -> Self {
        match id {
            431 => Self::AtomicPath,
            432 => Self::BaseAtomicExpr,
            433 => Self::LANGBRACKET_FullTypeExpr_RANGBRACKET_AtomicExpr_P_TYPECAST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AtomicPath {
    fn from_id(id: usize) -> Self {
        match id {
            434 => Self::AtomicExpr_PathStep_P_DOT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseAtomicExpr {
    fn from_id(id: usize) -> Self {
        match id {
            435 => Self::Collection,
            436 => Self::Constant,
            437 => Self::DUNDERDEFAULT,
            438 => Self::DUNDERNEW,
            439 => Self::DUNDEROLD,
            440 => Self::DUNDERSOURCE,
            441 => Self::DUNDERSPECIFIED,
            442 => Self::DUNDERSUBJECT,
            443 => Self::FreeShape,
            444 => Self::FuncExpr,
            445 => Self::NamedTuple,
            446 => Self::NodeName_P_DOT,
            447 => Self::ParenExpr_P_UMINUS,
            448 => Self::PathStep_P_DOT,
            449 => Self::Set,
            450 => Self::Tuple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseBooleanConstant {
    fn from_id(id: usize) -> Self {
        match id {
            451 => Self::FALSE,
            452 => Self::TRUE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseBytesConstant {
    fn from_id(id: usize) -> Self {
        match id {
            453 => Self::BCONST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseName {
    fn from_id(id: usize) -> Self {
        match id {
            454 => Self::Identifier,
            455 => Self::QualifiedName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseNumberConstant {
    fn from_id(id: usize) -> Self {
        match id {
            456 => Self::FCONST,
            457 => Self::ICONST,
            458 => Self::NFCONST,
            459 => Self::NICONST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseStringConstant {
    fn from_id(id: usize) -> Self {
        match id {
            460 => Self::SCONST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BranchOptions {
    fn from_id(id: usize) -> Self {
        match id {
            461 => Self::FORCE,
            462 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            463 => Self::AlterBranchStmt,
            464 => Self::CreateBranchStmt,
            465 => Self::DropBranchStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ByClause {
    fn from_id(id: usize) -> Self {
        match id {
            466 => Self::BY_GroupingElementList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CastAllowedUse {
    fn from_id(id: usize) -> Self {
        match id {
            467 => Self::ALLOW_ASSIGNMENT,
            468 => Self::ALLOW_IMPLICIT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CastCode {
    fn from_id(id: usize) -> Self {
        match id {
            469 => Self::USING_Identifier_BaseStringConstant,
            470 => Self::USING_Identifier_CAST,
            471 => Self::USING_Identifier_EXPRESSION,
            472 => Self::USING_Identifier_FUNCTION_BaseStringConstant,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Collection {
    fn from_id(id: usize) -> Self {
        match id {
            473 => Self::LBRACKET_OptExprList_RBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CollectionTypeName {
    fn from_id(id: usize) -> Self {
        match id {
            474 => Self::NodeName_LANGBRACKET_RANGBRACKET,
            475 => Self::NodeName_LANGBRACKET_SubtypeList_RANGBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ColonedIdents {
    fn from_id(id: usize) -> Self {
        match id {
            476 => Self::AnyIdentifier,
            477 => Self::ColonedIdents_DOUBLECOLON_AnyIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CommitMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            478 => Self::COMMIT_MIGRATION,
            479 => Self::COMMIT_MIGRATION_REWRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CompareOp {
    fn from_id(id: usize) -> Self {
        match id {
            480 => Self::DISTINCTFROM_P_COMPARE_OP,
            481 => Self::EQUALS_P_COMPARE_OP,
            482 => Self::GREATEREQ_P_COMPARE_OP,
            483 => Self::LANGBRACKET_P_COMPARE_OP,
            484 => Self::LESSEQ_P_COMPARE_OP,
            485 => Self::NOTDISTINCTFROM_P_COMPARE_OP,
            486 => Self::NOTEQ_P_COMPARE_OP,
            487 => Self::RANGBRACKET_P_COMPARE_OP,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ComputableShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            488 => Self::MULTI_SimpleShapePointer_ASSIGN_Expr,
            489 => Self::OPTIONAL_MULTI_SimpleShapePointer_ASSIGN_Expr,
            490 => Self::OPTIONAL_SINGLE_SimpleShapePointer_ASSIGN_Expr,
            491 => Self::OPTIONAL_SimpleShapePointer_ASSIGN_Expr,
            492 => Self::REQUIRED_MULTI_SimpleShapePointer_ASSIGN_Expr,
            493 => Self::REQUIRED_SINGLE_SimpleShapePointer_ASSIGN_Expr,
            494 => Self::REQUIRED_SimpleShapePointer_ASSIGN_Expr,
            495 => Self::SINGLE_SimpleShapePointer_ASSIGN_Expr,
            496 => Self::SimpleShapePointer_ADDASSIGN_Expr,
            497 => Self::SimpleShapePointer_ASSIGN_Expr,
            498 => Self::SimpleShapePointer_REMASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteConstraintBlock {
    fn from_id(id: usize) -> Self {
        match id {
            499 => Self::CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
            500 => Self::DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteConstraintShort {
    fn from_id(id: usize) -> Self {
        match id {
            501 => Self::CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
            502 => Self::DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteIndexDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            503 => Self::DEFERRED_INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            504 => Self::INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            505 => Self::DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            506 => Self::DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            507 => Self::INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            508 => Self::INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteIndexDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            509 => Self::DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr,
            510 => Self::DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
            511 => Self::INDEX_NodeName_OnExpr_OptExceptExpr,
            512 => Self::INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
            513 => Self::DEFERRED_INDEX_OnExpr_OptExceptExpr,
            514 => Self::INDEX_OnExpr_OptExceptExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteLinkBlock {
    fn from_id(id: usize) -> Self {
        match id {
            515 => Self::OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            516 => Self::OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            517 => Self::LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            518 => Self::PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteLinkShort {
    fn from_id(id: usize) -> Self {
        match id {
            519 => Self::LINK_PathNodeName_ASSIGN_Expr,
            520 => Self::OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
            521 => Self::OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
            522 => Self::PtrQuals_LINK_PathNodeName_ASSIGN_Expr,
            523 => Self::LINK_PathNodeName_OptExtendingSimple_PtrTarget,
            524 => Self::PtrQuals_LINK_PathNodeName_OptExtendingSimple_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcretePropertyBlock {
    fn from_id(id: usize) -> Self {
        match id {
            525 => Self::OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
            526 => Self::OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
            527 => Self::PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
            528 => Self::PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcretePropertyShort {
    fn from_id(id: usize) -> Self {
        match id {
            529 => Self::PROPERTY_PathNodeName_ASSIGN_Expr,
            530 => Self::OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
            531 => Self::OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
            532 => Self::PtrQuals_PROPERTY_PathNodeName_ASSIGN_Expr,
            533 => Self::PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
            534 => Self::PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteUnknownPointerBlock {
    fn from_id(id: usize) -> Self {
        match id {
            535 => Self::OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            536 => Self::OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            537 => Self::PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            538 => Self::PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteUnknownPointerObjectShort {
    fn from_id(id: usize) -> Self {
        match id {
            539 => Self::PathNodeName_ASSIGN_Expr,
            540 => Self::PtrQuals_PathNodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteUnknownPointerShort {
    fn from_id(id: usize) -> Self {
        match id {
            541 => Self::OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget,
            542 => Self::OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget,
            543 => Self::PathNodeName_OptExtendingSimple_PtrTarget,
            544 => Self::PtrQuals_PathNodeName_OptExtendingSimple_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConfigOp {
    fn from_id(id: usize) -> Self {
        match id {
            545 => Self::INSERT_NodeName_Shape,
            546 => Self::RESET_NodeName_OptFilterClause,
            547 => Self::SET_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConfigScope {
    fn from_id(id: usize) -> Self {
        match id {
            548 => Self::CURRENT_BRANCH,
            549 => Self::CURRENT_DATABASE,
            550 => Self::INSTANCE,
            551 => Self::SESSION,
            552 => Self::SYSTEM,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConfigStmt {
    fn from_id(id: usize) -> Self {
        match id {
            553 => Self::CONFIGURE_BRANCH_ConfigOp,
            554 => Self::CONFIGURE_ConfigScope_ConfigOp,
            555 => Self::CONFIGURE_DATABASE_ConfigOp,
            556 => Self::RESET_GLOBAL_NodeName,
            557 => Self::SET_GLOBAL_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Constant {
    fn from_id(id: usize) -> Self {
        match id {
            558 => Self::BaseBooleanConstant,
            559 => Self::BaseBytesConstant,
            560 => Self::BaseNumberConstant,
            561 => Self::BaseStringConstant,
            562 => Self::PARAMETER,
            563 => Self::PARAMETERANDTYPE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConstraintDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            564 => Self::ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
            565 => Self::ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConstraintDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            566 => Self::ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple,
            567 => Self::ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            568 => Self::CreateAnnotationValueStmt,
            569 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            570 => Self::LBRACE_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
            571 => Self::LBRACE_OptSemicolons_RBRACE,
            572 => Self::LBRACE_Semicolons_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            573 => Self::CreateAccessPolicyCommand,
            574 => Self::CreateAccessPolicyCommandsList_Semicolons_CreateAccessPolicyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            576 => Self::CreateAccessPolicySDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            577 => Self::SetAnnotation,
            578 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            579 => Self::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
            580 => Self::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
            581 => Self::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_RBRACE,
            582 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            583 => Self::CreateAccessPolicySDLCommandFull,
            584 => Self::CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            585 => Self::CREATE_ACCESS_POLICY_UnqualifiedPointerName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_OptCreateAccessPolicyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasCommand {
    fn from_id(id: usize) -> Self {
        match id {
            586 => Self::AlterAnnotationValueStmt,
            587 => Self::CreateAnnotationValueStmt,
            588 => Self::SetFieldStmt,
            589 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            590 => Self::CreateAliasCommand,
            591 => Self::LBRACE_CreateAliasCommandsList_OptSemicolons_RBRACE,
            592 => Self::LBRACE_OptSemicolons_RBRACE,
            593 => Self::LBRACE_Semicolons_CreateAliasCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            594 => Self::CreateAliasCommand,
            595 => Self::CreateAliasCommandsList_Semicolons_CreateAliasCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            597 => Self::CreateAliasSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            598 => Self::SetAnnotation,
            599 => Self::SetField,
            600 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            601 => Self::LBRACE_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
            602 => Self::LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
            603 => Self::LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_RBRACE,
            604 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            605 => Self::CreateAliasSDLCommandFull,
            606 => Self::CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSingleSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            608 => Self::CreateAliasSDLCommandShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasStmt {
    fn from_id(id: usize) -> Self {
        match id {
            609 => Self::CREATE_ALIAS_NodeName_CreateAliasCommandsBlock,
            610 => Self::CREATE_ALIAS_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            611 => Self::CreateAnnotationValueStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            612 => Self::LBRACE_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
            613 => Self::LBRACE_OptSemicolons_RBRACE,
            614 => Self::LBRACE_Semicolons_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            615 => Self::CreateAnnotationCommand,
            616 => Self::CreateAnnotationCommandsList_Semicolons_CreateAnnotationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            617 => Self::CREATE_ABSTRACT_ANNOTATION_NodeName_OptCreateAnnotationCommandsBlock,
            618 => Self::CREATE_ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationValueStmt {
    fn from_id(id: usize) -> Self {
        match id {
            619 => Self::CREATE_ANNOTATION_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateBranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            620 => Self::CREATE_EMPTY_BRANCH_DatabaseName,
            621 => Self::CREATE_DATA_BRANCH_DatabaseName_FROM_DatabaseName,
            622 => Self::CREATE_SCHEMA_BRANCH_DatabaseName_FROM_DatabaseName,
            623 => Self::CREATE_TEMPLATE_BRANCH_DatabaseName_FROM_DatabaseName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastCommand {
    fn from_id(id: usize) -> Self {
        match id {
            624 => Self::AlterAnnotationValueStmt,
            625 => Self::CastAllowedUse,
            626 => Self::CastCode,
            627 => Self::CreateAnnotationValueStmt,
            628 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            629 => Self::CreateCastCommand,
            630 => Self::LBRACE_CreateCastCommandsList_OptSemicolons_RBRACE,
            631 => Self::LBRACE_OptSemicolons_RBRACE,
            632 => Self::LBRACE_Semicolons_CreateCastCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            633 => Self::CreateCastCommand,
            634 => Self::CreateCastCommandsList_Semicolons_CreateCastCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastStmt {
    fn from_id(id: usize) -> Self {
        match id {
            635 => Self::CREATE_CAST_FROM_TypeName_TO_TypeName_CreateCastCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCommand {
    fn from_id(id: usize) -> Self {
        match id {
            636 => Self::AlterAnnotationValueStmt,
            637 => Self::CreateAnnotationValueStmt,
            638 => Self::SetFieldStmt,
            639 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            640 => Self::LBRACE_CreateCommandsList_OptSemicolons_RBRACE,
            641 => Self::LBRACE_OptSemicolons_RBRACE,
            642 => Self::LBRACE_Semicolons_CreateCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            643 => Self::CreateCommand,
            644 => Self::CreateCommandsList_Semicolons_CreateCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            645 => Self::CREATE_OptDelegated_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            647 => Self::CreateConcreteIndexSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            648 => Self::SetAnnotation,
            649 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            650 => Self::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
            651 => Self::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
            652 => Self::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_RBRACE,
            653 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            654 => Self::CreateConcreteIndexSDLCommandFull,
            655 => Self::CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            656 => Self::CREATE_OptDeferred_INDEX_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
            657 => Self::CREATE_OptDeferred_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            658 => Self::AlterAnnotationValueStmt,
            659 => Self::CreateAnnotationValueStmt,
            660 => Self::CreateConcreteConstraintStmt,
            661 => Self::CreateConcreteIndexStmt,
            662 => Self::CreateConcretePropertyStmt,
            663 => Self::CreateRewriteStmt,
            664 => Self::CreateSimpleExtending,
            665 => Self::OnSourceDeleteStmt,
            666 => Self::OnTargetDeleteStmt,
            667 => Self::SetFieldStmt,
            668 => Self::SetRequiredInCreateStmt,
            669 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            670 => Self::LBRACE_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
            671 => Self::LBRACE_OptSemicolons_RBRACE,
            672 => Self::LBRACE_Semicolons_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            673 => Self::CreateConcreteLinkCommand,
            674 => Self::CreateConcreteLinkCommandsList_Semicolons_CreateConcreteLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            675 => Self::ConcreteConstraintBlock,
            676 => Self::ConcreteIndexDeclarationBlock,
            677 => Self::ConcretePropertyBlock,
            678 => Self::ConcreteUnknownPointerBlock,
            679 => Self::RewriteDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            680 => Self::CreateConcreteLinkSDLCommandBlock,
            681 => Self::CreateConcreteLinkSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            682 => Self::ConcreteConstraintShort,
            683 => Self::ConcreteIndexDeclarationShort,
            684 => Self::ConcretePropertyShort,
            685 => Self::ConcreteUnknownPointerShort,
            686 => Self::CreateSimpleExtending,
            687 => Self::OnSourceDeleteStmt,
            688 => Self::OnTargetDeleteStmt,
            689 => Self::RewriteDeclarationShort,
            690 => Self::SetAnnotation,
            691 => Self::SetField,
            692 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            693 => Self::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
            694 => Self::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
            695 => Self::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_RBRACE,
            696 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            697 => Self::CreateConcreteLinkSDLCommandFull,
            698 => Self::CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            699 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_ASSIGN_Expr,
            700 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptCreateConcreteLinkCommandsBlock,
            701 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
            702 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            703 => Self::AlterAnnotationValueStmt,
            704 => Self::CreateAnnotationValueStmt,
            705 => Self::CreateConcreteConstraintStmt,
            706 => Self::CreateRewriteStmt,
            707 => Self::CreateSimpleExtending,
            708 => Self::SetFieldStmt,
            709 => Self::SetRequiredInCreateStmt,
            710 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            711 => Self::LBRACE_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
            712 => Self::LBRACE_OptSemicolons_RBRACE,
            713 => Self::LBRACE_Semicolons_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            714 => Self::CreateConcretePropertyCommand,
            715 => Self::CreateConcretePropertyCommandsList_Semicolons_CreateConcretePropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            716 => Self::ConcreteConstraintBlock,
            717 => Self::RewriteDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            718 => Self::CreateConcretePropertySDLCommandBlock,
            719 => Self::CreateConcretePropertySDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            720 => Self::ConcreteConstraintShort,
            721 => Self::CreateSimpleExtending,
            722 => Self::RewriteDeclarationShort,
            723 => Self::SetAnnotation,
            724 => Self::SetField,
            725 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            726 => Self::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
            727 => Self::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
            728 => Self::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_RBRACE,
            729 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            730 => Self::CreateConcretePropertySDLCommandFull,
            731 => Self::CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            732 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_ASSIGN_Expr,
            733 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptCreateConcretePropertyCommandsBlock,
            734 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
            735 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            736 => Self::CREATE_ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
            737 => Self::CREATE_ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseCommand {
    fn from_id(id: usize) -> Self {
        match id {
            738 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            739 => Self::LBRACE_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
            740 => Self::LBRACE_OptSemicolons_RBRACE,
            741 => Self::LBRACE_Semicolons_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            742 => Self::CreateDatabaseCommand,
            743 => Self::CreateDatabaseCommandsList_Semicolons_CreateDatabaseCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            744 => Self::CREATE_DATABASE_DatabaseName_FROM_AnyNodeName_OptCreateDatabaseCommandsBlock,
            745 => Self::CREATE_DATABASE_DatabaseName_OptCreateDatabaseCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionCommand {
    fn from_id(id: usize) -> Self {
        match id {
            746 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            747 => Self::LBRACE_CreateExtensionCommandsList_OptSemicolons_RBRACE,
            748 => Self::LBRACE_OptSemicolons_RBRACE,
            749 => Self::LBRACE_Semicolons_CreateExtensionCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            750 => Self::CreateExtensionCommand,
            751 => Self::CreateExtensionCommandsList_Semicolons_CreateExtensionCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageCommand {
    fn from_id(id: usize) -> Self {
        match id {
            752 => Self::NestedQLBlockStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            753 => Self::LBRACE_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
            754 => Self::LBRACE_OptSemicolons_RBRACE,
            755 => Self::LBRACE_Semicolons_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            756 => Self::CreateExtensionPackageCommand,
            757 => Self::CreateExtensionPackageCommandsList_Semicolons_CreateExtensionPackageCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            758 => Self::CREATE_EXTENSIONPACKAGE_ShortNodeName_MIGRATION_FROM_ExtensionVersion_TO_ExtensionVersion_OptCreateExtensionPackageCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageStmt {
    fn from_id(id: usize) -> Self {
        match id {
            759 => Self::CREATE_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion_OptCreateExtensionPackageCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            760 => Self::CREATE_EXTENSION_ShortNodeName_OptExtensionVersion_OptCreateExtensionCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionArgs {
    fn from_id(id: usize) -> Self {
        match id {
            761 => Self::LPAREN_FuncDeclArgs_RPAREN,
            762 => Self::LPAREN_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionCommand {
    fn from_id(id: usize) -> Self {
        match id {
            763 => Self::AlterAnnotationValueStmt,
            764 => Self::CreateAnnotationValueStmt,
            765 => Self::FromFunction,
            766 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            767 => Self::CreateFunctionCommand,
            768 => Self::LBRACE_CreateFunctionCommandsList_OptSemicolons_RBRACE,
            769 => Self::LBRACE_OptSemicolons_RBRACE,
            770 => Self::LBRACE_Semicolons_CreateFunctionCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            771 => Self::CreateFunctionCommand,
            772 => Self::CreateFunctionCommandsList_Semicolons_CreateFunctionCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            774 => Self::CreateFunctionSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            775 => Self::FromFunction,
            776 => Self::SetAnnotation,
            777 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            778 => Self::LBRACE_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
            779 => Self::LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
            780 => Self::LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_RBRACE,
            781 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            782 => Self::CreateFunctionSDLCommandFull,
            783 => Self::CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSingleSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            785 => Self::CreateFunctionSDLCommandShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            786 => Self::CREATE_FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFutureStmt {
    fn from_id(id: usize) -> Self {
        match id {
            787 => Self::CREATE_FUTURE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalCommand {
    fn from_id(id: usize) -> Self {
        match id {
            788 => Self::CreateAnnotationValueStmt,
            789 => Self::SetFieldStmt,
            790 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            791 => Self::LBRACE_CreateGlobalCommandsList_OptSemicolons_RBRACE,
            792 => Self::LBRACE_OptSemicolons_RBRACE,
            793 => Self::LBRACE_Semicolons_CreateGlobalCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            794 => Self::CreateGlobalCommand,
            795 => Self::CreateGlobalCommandsList_Semicolons_CreateGlobalCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            797 => Self::CreateGlobalSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            798 => Self::SetAnnotation,
            799 => Self::SetField,
            800 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            801 => Self::LBRACE_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
            802 => Self::LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
            803 => Self::LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_RBRACE,
            804 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            805 => Self::CreateGlobalSDLCommandFull,
            806 => Self::CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalStmt {
    fn from_id(id: usize) -> Self {
        match id {
            807 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
            808 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_OptCreateConcretePropertyCommandsBlock,
            809 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_ARROW_FullTypeExpr_OptCreateGlobalCommandsBlock,
            810 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_COLON_FullTypeExpr_OptCreateGlobalCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            811 => Self::AlterAnnotationValueStmt,
            812 => Self::CreateAnnotationValueStmt,
            813 => Self::SetFieldStmt,
            814 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            815 => Self::LBRACE_CreateIndexCommandsList_OptSemicolons_RBRACE,
            816 => Self::LBRACE_OptSemicolons_RBRACE,
            817 => Self::LBRACE_Semicolons_CreateIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            818 => Self::CreateIndexCommand,
            819 => Self::CreateIndexCommandsList_Semicolons_CreateIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchCommand {
    fn from_id(id: usize) -> Self {
        match id {
            820 => Self::CreateAnnotationValueStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            821 => Self::LBRACE_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
            822 => Self::LBRACE_OptSemicolons_RBRACE,
            823 => Self::LBRACE_Semicolons_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            824 => Self::CreateIndexMatchCommand,
            825 => Self::CreateIndexMatchCommandsList_Semicolons_CreateIndexMatchCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            826 => Self::CREATE_INDEX_MATCH_FOR_TypeName_USING_NodeName_OptCreateIndexMatchCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            828 => Self::CreateIndexSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            829 => Self::SetAnnotation,
            830 => Self::SetField,
            831 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            832 => Self::LBRACE_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
            833 => Self::LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
            834 => Self::LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_RBRACE,
            835 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            836 => Self::CreateIndexSDLCommandFull,
            837 => Self::CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            838 => Self::CREATE_ABSTRACT_INDEX_NodeName_OptExtendingSimple_OptCreateIndexCommandsBlock,
            839 => Self::CREATE_ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_OptCreateIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            840 => Self::AlterAnnotationValueStmt,
            841 => Self::CreateAnnotationValueStmt,
            842 => Self::CreateConcreteConstraintStmt,
            843 => Self::CreateConcreteIndexStmt,
            844 => Self::CreateConcretePropertyStmt,
            845 => Self::CreateRewriteStmt,
            846 => Self::CreateSimpleExtending,
            847 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            848 => Self::LBRACE_CreateLinkCommandsList_OptSemicolons_RBRACE,
            849 => Self::LBRACE_OptSemicolons_RBRACE,
            850 => Self::LBRACE_Semicolons_CreateLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            851 => Self::CreateLinkCommand,
            852 => Self::CreateLinkCommandsList_Semicolons_CreateLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            853 => Self::ConcreteConstraintBlock,
            854 => Self::ConcreteIndexDeclarationBlock,
            855 => Self::ConcretePropertyBlock,
            856 => Self::ConcreteUnknownPointerBlock,
            857 => Self::RewriteDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            858 => Self::CreateLinkSDLCommandBlock,
            859 => Self::CreateLinkSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            860 => Self::ConcreteConstraintShort,
            861 => Self::ConcreteIndexDeclarationShort,
            862 => Self::ConcretePropertyShort,
            863 => Self::ConcreteUnknownPointerShort,
            864 => Self::CreateSimpleExtending,
            865 => Self::RewriteDeclarationShort,
            866 => Self::SetAnnotation,
            867 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            868 => Self::LBRACE_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
            869 => Self::LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
            870 => Self::LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_RBRACE,
            871 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            872 => Self::CreateLinkSDLCommandFull,
            873 => Self::CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            874 => Self::CREATE_ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_OptCreateLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            875 => Self::NestedQLBlockStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            876 => Self::LBRACE_CreateMigrationCommandsList_OptSemicolons_RBRACE,
            877 => Self::LBRACE_OptSemicolons_RBRACE,
            878 => Self::LBRACE_Semicolons_CreateMigrationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            879 => Self::CreateMigrationCommand,
            880 => Self::CreateMigrationCommandsList_Semicolons_CreateMigrationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            881 => Self::CREATE_APPLIED_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
            882 => Self::CREATE_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateModuleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            883 => Self::CREATE_MODULE_ModuleName_OptIfNotExists_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            884 => Self::AlterAccessPolicyStmt,
            885 => Self::AlterAnnotationValueStmt,
            886 => Self::AlterConcreteConstraintStmt,
            887 => Self::AlterConcreteIndexStmt,
            888 => Self::AlterConcreteLinkStmt,
            889 => Self::AlterConcretePropertyStmt,
            890 => Self::AlterTriggerStmt,
            891 => Self::CreateAccessPolicyStmt,
            892 => Self::CreateAnnotationValueStmt,
            893 => Self::CreateConcreteConstraintStmt,
            894 => Self::CreateConcreteIndexStmt,
            895 => Self::CreateConcreteLinkStmt,
            896 => Self::CreateConcretePropertyStmt,
            897 => Self::CreateTriggerStmt,
            898 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            899 => Self::LBRACE_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
            900 => Self::LBRACE_OptSemicolons_RBRACE,
            901 => Self::LBRACE_Semicolons_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            902 => Self::CreateObjectTypeCommand,
            903 => Self::CreateObjectTypeCommandsList_Semicolons_CreateObjectTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            904 => Self::AccessPolicyDeclarationBlock,
            905 => Self::ConcreteConstraintBlock,
            906 => Self::ConcreteIndexDeclarationBlock,
            907 => Self::ConcreteLinkBlock,
            908 => Self::ConcretePropertyBlock,
            909 => Self::ConcreteUnknownPointerBlock,
            910 => Self::TriggerDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            911 => Self::CreateObjectTypeSDLCommandBlock,
            912 => Self::CreateObjectTypeSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            913 => Self::AccessPolicyDeclarationShort,
            914 => Self::ConcreteConstraintShort,
            915 => Self::ConcreteIndexDeclarationShort,
            916 => Self::ConcreteLinkShort,
            917 => Self::ConcretePropertyShort,
            918 => Self::ConcreteUnknownPointerObjectShort,
            919 => Self::ConcreteUnknownPointerShort,
            920 => Self::SetAnnotation,
            921 => Self::TriggerDeclarationShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            922 => Self::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
            923 => Self::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
            924 => Self::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_RBRACE,
            925 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            926 => Self::CreateObjectTypeSDLCommandFull,
            927 => Self::CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            928 => Self::CREATE_ABSTRACT_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
            929 => Self::CREATE_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorCommand {
    fn from_id(id: usize) -> Self {
        match id {
            930 => Self::AlterAnnotationValueStmt,
            931 => Self::CreateAnnotationValueStmt,
            932 => Self::OperatorCode,
            933 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            934 => Self::CreateOperatorCommand,
            935 => Self::LBRACE_CreateOperatorCommandsList_OptSemicolons_RBRACE,
            936 => Self::LBRACE_OptSemicolons_RBRACE,
            937 => Self::LBRACE_Semicolons_CreateOperatorCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            938 => Self::CreateOperatorCommand,
            939 => Self::CreateOperatorCommandsList_Semicolons_CreateOperatorCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorStmt {
    fn from_id(id: usize) -> Self {
        match id {
            940 => Self::CREATE_ABSTRACT_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_OptCreateOperatorCommandsBlock,
            941 => Self::CREATE_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateOperatorCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            942 => Self::AlterAnnotationValueStmt,
            943 => Self::CreateAnnotationValueStmt,
            944 => Self::CreateSimpleExtending,
            945 => Self::SetFieldStmt,
            946 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            947 => Self::LBRACE_CreatePropertyCommandsList_OptSemicolons_RBRACE,
            948 => Self::LBRACE_OptSemicolons_RBRACE,
            949 => Self::LBRACE_Semicolons_CreatePropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            950 => Self::CreatePropertyCommand,
            951 => Self::CreatePropertyCommandsList_Semicolons_CreatePropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            953 => Self::CreatePropertySDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            954 => Self::CreateSimpleExtending,
            955 => Self::SetAnnotation,
            956 => Self::SetField,
            957 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            958 => Self::LBRACE_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
            959 => Self::LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
            960 => Self::LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_RBRACE,
            961 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            962 => Self::CreatePropertySDLCommandFull,
            963 => Self::CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            964 => Self::CREATE_ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_OptCreatePropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            965 => Self::AlterAnnotationValueStmt,
            966 => Self::CreateAnnotationValueStmt,
            967 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            968 => Self::LBRACE_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
            969 => Self::LBRACE_OptSemicolons_RBRACE,
            970 => Self::LBRACE_Semicolons_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            971 => Self::CreatePseudoTypeCommand,
            972 => Self::CreatePseudoTypeCommandsList_Semicolons_CreatePseudoTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            973 => Self::CREATE_PSEUDO_TYPE_NodeName_OptCreatePseudoTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteCommand {
    fn from_id(id: usize) -> Self {
        match id {
            974 => Self::CreateAnnotationValueStmt,
            975 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            976 => Self::LBRACE_CreateRewriteCommandsList_OptSemicolons_RBRACE,
            977 => Self::LBRACE_OptSemicolons_RBRACE,
            978 => Self::LBRACE_Semicolons_CreateRewriteCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            979 => Self::CreateRewriteCommand,
            980 => Self::CreateRewriteCommandsList_Semicolons_CreateRewriteCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            982 => Self::CreateRewriteSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            983 => Self::SetAnnotation,
            984 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            985 => Self::LBRACE_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
            986 => Self::LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
            987 => Self::LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_RBRACE,
            988 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            989 => Self::CreateRewriteSDLCommandFull,
            990 => Self::CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            991 => Self::CREATE_REWRITE_RewriteKindList_USING_ParenExpr_OptCreateRewriteCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleCommand {
    fn from_id(id: usize) -> Self {
        match id {
            992 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            993 => Self::LBRACE_CreateRoleCommandsList_OptSemicolons_RBRACE,
            994 => Self::LBRACE_OptSemicolons_RBRACE,
            995 => Self::LBRACE_Semicolons_CreateRoleCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            996 => Self::CreateRoleCommand,
            997 => Self::CreateRoleCommandsList_Semicolons_CreateRoleCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            998 => Self::CREATE_OptSuperuser_ROLE_ShortNodeName_OptShortExtending_OptIfNotExists_OptCreateRoleCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            1000 => Self::CreateSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            1001 => Self::SetAnnotation,
            1002 => Self::SetField,
            1003 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1004 => Self::LBRACE_OptSemicolons_CreateSDLCommandShort_RBRACE,
            1005 => Self::LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_CreateSDLCommandShort_RBRACE,
            1006 => Self::LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_RBRACE,
            1007 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1008 => Self::CreateSDLCommandFull,
            1009 => Self::CreateSDLCommandsList_OptSemicolons_CreateSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1010 => Self::AlterAnnotationValueStmt,
            1011 => Self::CreateAnnotationValueStmt,
            1012 => Self::CreateConcreteConstraintStmt,
            1013 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1014 => Self::LBRACE_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
            1015 => Self::LBRACE_OptSemicolons_RBRACE,
            1016 => Self::LBRACE_Semicolons_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1017 => Self::CreateScalarTypeCommand,
            1018 => Self::CreateScalarTypeCommandsList_Semicolons_CreateScalarTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1019 => Self::ConcreteConstraintBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            1020 => Self::CreateScalarTypeSDLCommandBlock,
            1021 => Self::CreateScalarTypeSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            1022 => Self::ConcreteConstraintShort,
            1023 => Self::SetAnnotation,
            1024 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1025 => Self::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
            1026 => Self::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
            1027 => Self::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_RBRACE,
            1028 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1029 => Self::CreateScalarTypeSDLCommandFull,
            1030 => Self::CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1031 => Self::CREATE_ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
            1032 => Self::CREATE_FINAL_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
            1033 => Self::CREATE_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSimpleExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1034 => Self::EXTENDING_SimpleTypeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1035 => Self::CreateAnnotationValueStmt,
            1036 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1037 => Self::LBRACE_CreateTriggerCommandsList_OptSemicolons_RBRACE,
            1038 => Self::LBRACE_OptSemicolons_RBRACE,
            1039 => Self::LBRACE_Semicolons_CreateTriggerCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1040 => Self::CreateTriggerCommand,
            1041 => Self::CreateTriggerCommandsList_Semicolons_CreateTriggerCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            1043 => Self::CreateTriggerSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            1044 => Self::SetAnnotation,
            1045 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1046 => Self::LBRACE_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
            1047 => Self::LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
            1048 => Self::LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_RBRACE,
            1049 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1050 => Self::CreateTriggerSDLCommandFull,
            1051 => Self::CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1052 => Self::CREATE_TRIGGER_UnqualifiedPointerName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_OptCreateTriggerCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1053 => Self::BranchStmt,
            1054 => Self::DatabaseStmt,
            1055 => Self::ExtensionPackageStmt,
            1056 => Self::MigrationStmt,
            1057 => Self::OptWithDDLStmt,
            1058 => Self::RoleStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DDLWithBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1059 => Self::WithBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DatabaseName {
    fn from_id(id: usize) -> Self {
        match id {
            1060 => Self::Identifier,
            1061 => Self::ReservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1062 => Self::AlterDatabaseStmt,
            1063 => Self::CreateDatabaseStmt,
            1064 => Self::DropDatabaseStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DescribeFormat {
    fn from_id(id: usize) -> Self {
        match id {
            1065 => Self::AS_DDL,
            1066 => Self::AS_JSON,
            1067 => Self::AS_SDL,
            1068 => Self::AS_TEXT,
            1069 => Self::AS_TEXT_VERBOSE,
            1070 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DescribeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1071 => Self::DESCRIBE_CURRENT_BRANCH_CONFIG_DescribeFormat,
            1072 => Self::DESCRIBE_CURRENT_DATABASE_CONFIG_DescribeFormat,
            1073 => Self::DESCRIBE_CURRENT_MIGRATION_DescribeFormat,
            1074 => Self::DESCRIBE_INSTANCE_CONFIG_DescribeFormat,
            1075 => Self::DESCRIBE_OBJECT_NodeName_DescribeFormat,
            1076 => Self::DESCRIBE_ROLES_DescribeFormat,
            1077 => Self::DESCRIBE_SCHEMA_DescribeFormat,
            1078 => Self::DESCRIBE_SYSTEM_CONFIG_DescribeFormat,
            1079 => Self::DESCRIBE_SchemaItem_DescribeFormat,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DotName {
    fn from_id(id: usize) -> Self {
        match id {
            1080 => Self::DottedIdents,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DottedIdents {
    fn from_id(id: usize) -> Self {
        match id {
            1081 => Self::AnyIdentifier,
            1082 => Self::DottedIdents_DOT_AnyIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAccessPolicyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1083 => Self::DROP_ACCESS_POLICY_UnqualifiedPointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAliasStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1084 => Self::DROP_ALIAS_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAnnotationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1085 => Self::DROP_ABSTRACT_ANNOTATION_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAnnotationValueStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1086 => Self::DROP_ANNOTATION_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropBranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1087 => Self::DROP_BRANCH_DatabaseName_BranchOptions,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropCastStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1088 => Self::DROP_CAST_FROM_TypeName_TO_TypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1089 => Self::DROP_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1090 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1091 => Self::LBRACE_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
            1092 => Self::LBRACE_OptSemicolons_RBRACE,
            1093 => Self::LBRACE_Semicolons_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1094 => Self::DropConcreteIndexCommand,
            1095 => Self::DropConcreteIndexCommandsList_Semicolons_DropConcreteIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1096 => Self::DROP_INDEX_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
            1097 => Self::DROP_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1098 => Self::DropConcreteConstraintStmt,
            1099 => Self::DropConcreteIndexStmt,
            1100 => Self::DropConcretePropertyStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1101 => Self::LBRACE_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
            1102 => Self::LBRACE_OptSemicolons_RBRACE,
            1103 => Self::LBRACE_Semicolons_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1104 => Self::DropConcreteLinkCommand,
            1105 => Self::DropConcreteLinkCommandsList_Semicolons_DropConcreteLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1106 => Self::DROP_LINK_UnqualifiedPointerName_OptDropConcreteLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcretePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1107 => Self::DROP_PROPERTY_UnqualifiedPointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1108 => Self::DROP_ABSTRACT_CONSTRAINT_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropDatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1109 => Self::DROP_DATABASE_DatabaseName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropExtensionPackageMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1110 => Self::DROP_EXTENSIONPACKAGE_ShortNodeName_MIGRATION_FROM_ExtensionVersion_TO_ExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropExtensionPackageStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1111 => Self::DROP_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1112 => Self::DROP_EXTENSION_ShortNodeName_OptExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropFunctionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1113 => Self::DROP_FUNCTION_NodeName_CreateFunctionArgs,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropFutureStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1114 => Self::DROP_FUTURE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropGlobalStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1115 => Self::DROP_GLOBAL_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropIndexMatchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1116 => Self::DROP_INDEX_MATCH_FOR_TypeName_USING_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1117 => Self::DROP_ABSTRACT_INDEX_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1118 => Self::DropConcreteConstraintStmt,
            1119 => Self::DropConcreteIndexStmt,
            1120 => Self::DropConcretePropertyStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1121 => Self::LBRACE_DropLinkCommandsList_OptSemicolons_RBRACE,
            1122 => Self::LBRACE_OptSemicolons_RBRACE,
            1123 => Self::LBRACE_Semicolons_DropLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1124 => Self::DropLinkCommand,
            1125 => Self::DropLinkCommandsList_Semicolons_DropLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1126 => Self::DROP_ABSTRACT_LINK_PtrNodeName_OptDropLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1127 => Self::DROP_MIGRATION_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropModuleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1128 => Self::DROP_MODULE_ModuleName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1129 => Self::DropConcreteConstraintStmt,
            1130 => Self::DropConcreteIndexStmt,
            1131 => Self::DropConcreteLinkStmt,
            1132 => Self::DropConcretePropertyStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1133 => Self::LBRACE_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
            1134 => Self::LBRACE_OptSemicolons_RBRACE,
            1135 => Self::LBRACE_Semicolons_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1136 => Self::DropObjectTypeCommand,
            1137 => Self::DropObjectTypeCommandsList_Semicolons_DropObjectTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1138 => Self::DROP_TYPE_NodeName_OptDropObjectTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropOperatorStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1139 => Self::DROP_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropPropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1140 => Self::DROP_ABSTRACT_PROPERTY_PtrNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropRewriteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1141 => Self::DROP_REWRITE_RewriteKindList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropRoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1142 => Self::DROP_ROLE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropScalarTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1143 => Self::DROP_SCALAR_TYPE_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropTriggerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1144 => Self::DROP_TRIGGER_UnqualifiedPointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::EdgeQLBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1145 => Self::OptSemicolons,
            1146 => Self::StatementBlock_OptSemicolons,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::EdgeQLGrammar {
    fn from_id(id: usize) -> Self {
        match id {
            1147 => Self::STARTBLOCK_EdgeQLBlock_EOI,
            1148 => Self::STARTEXTENSION_CreateExtensionPackageCommandsBlock_EOI,
            1149 => Self::STARTFRAGMENT_ExprStmt_EOI,
            1150 => Self::STARTFRAGMENT_Expr_EOI,
            1151 => Self::STARTMIGRATION_CreateMigrationCommandsBlock_EOI,
            1152 => Self::STARTSDLDOCUMENT_SDLDocument_EOI,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Expr {
    fn from_id(id: usize) -> Self {
        match id {
            1153 => Self::BaseAtomicExpr,
            1154 => Self::DETACHED_Expr,
            1155 => Self::DISTINCT_Expr,
            1156 => Self::EXISTS_Expr,
            1157 => Self::Expr_AND_Expr,
            1158 => Self::Expr_CIRCUMFLEX_Expr,
            1159 => Self::Expr_CompareOp_Expr_P_COMPARE_OP,
            1160 => Self::Expr_DOUBLEPLUS_Expr,
            1161 => Self::Expr_DOUBLEQMARK_Expr_P_DOUBLEQMARK_OP,
            1162 => Self::Expr_DOUBLESLASH_Expr,
            1163 => Self::Expr_EXCEPT_Expr,
            1164 => Self::Expr_IF_Expr_ELSE_Expr,
            1165 => Self::Expr_ILIKE_Expr,
            1166 => Self::Expr_INTERSECT_Expr,
            1167 => Self::Expr_IN_Expr,
            1168 => Self::Expr_IS_NOT_TypeExpr_P_IS,
            1169 => Self::Expr_IS_TypeExpr,
            1170 => Self::Expr_IndirectionEl,
            1171 => Self::Expr_LIKE_Expr,
            1172 => Self::Expr_MINUS_Expr,
            1173 => Self::Expr_NOT_ILIKE_Expr,
            1174 => Self::Expr_NOT_IN_Expr_P_IN,
            1175 => Self::Expr_NOT_LIKE_Expr,
            1176 => Self::Expr_OR_Expr,
            1177 => Self::Expr_PERCENT_Expr,
            1178 => Self::Expr_PLUS_Expr,
            1179 => Self::Expr_SLASH_Expr,
            1180 => Self::Expr_STAR_Expr,
            1181 => Self::Expr_Shape,
            1182 => Self::Expr_UNION_Expr,
            1183 => Self::GLOBAL_NodeName,
            1184 => Self::INTROSPECT_TypeExpr,
            1185 => Self::IfThenElseExpr,
            1186 => Self::LANGBRACKET_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
            1187 => Self::LANGBRACKET_OPTIONAL_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
            1188 => Self::LANGBRACKET_REQUIRED_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
            1189 => Self::MINUS_Expr_P_UMINUS,
            1190 => Self::NOT_Expr,
            1191 => Self::PLUS_Expr_P_UMINUS,
            1192 => Self::Path,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprList {
    fn from_id(id: usize) -> Self {
        match id {
            1193 => Self::ExprListInner,
            1194 => Self::ExprListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1195 => Self::Expr,
            1196 => Self::ExprListInner_COMMA_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1197 => Self::ExprStmtCore,
            1198 => Self::WithBlock_ExprStmtCore,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprStmtCore {
    fn from_id(id: usize) -> Self {
        match id {
            1199 => Self::InternalGroup,
            1200 => Self::SimpleDelete,
            1201 => Self::SimpleFor,
            1202 => Self::SimpleGroup,
            1203 => Self::SimpleInsert,
            1204 => Self::SimpleSelect,
            1205 => Self::SimpleUpdate,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Extending {
    fn from_id(id: usize) -> Self {
        match id {
            1206 => Self::EXTENDING_TypeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtendingSimple {
    fn from_id(id: usize) -> Self {
        match id {
            1207 => Self::EXTENDING_SimpleTypeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionPackageStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1208 => Self::CreateExtensionPackageMigrationStmt,
            1209 => Self::CreateExtensionPackageStmt,
            1210 => Self::DropExtensionPackageMigrationStmt,
            1211 => Self::DropExtensionPackageStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionRequirementDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1212 => Self::USING_EXTENSION_ShortNodeName_OptExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1213 => Self::AlterExtensionStmt,
            1214 => Self::CreateExtensionStmt,
            1215 => Self::DropExtensionStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionVersion {
    fn from_id(id: usize) -> Self {
        match id {
            1216 => Self::VERSION_BaseStringConstant,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FilterClause {
    fn from_id(id: usize) -> Self {
        match id {
            1217 => Self::FILTER_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeComputableShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1218 => Self::FreeSimpleShapePointer_ASSIGN_Expr,
            1219 => Self::MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
            1220 => Self::OPTIONAL_FreeSimpleShapePointer_ASSIGN_Expr,
            1221 => Self::OPTIONAL_MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
            1222 => Self::OPTIONAL_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
            1223 => Self::REQUIRED_FreeSimpleShapePointer_ASSIGN_Expr,
            1224 => Self::REQUIRED_MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
            1225 => Self::REQUIRED_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
            1226 => Self::SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeComputableShapePointerList {
    fn from_id(id: usize) -> Self {
        match id {
            1227 => Self::FreeComputableShapePointerListInner,
            1228 => Self::FreeComputableShapePointerListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeComputableShapePointerListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1229 => Self::FreeComputableShapePointer,
            1230 => Self::FreeComputableShapePointerListInner_COMMA_FreeComputableShapePointer,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeShape {
    fn from_id(id: usize) -> Self {
        match id {
            1231 => Self::LBRACE_FreeComputableShapePointerList_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeSimpleShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1232 => Self::FreeStepName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeStepName {
    fn from_id(id: usize) -> Self {
        match id {
            1233 => Self::DUNDERTYPE,
            1234 => Self::ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FromFunction {
    fn from_id(id: usize) -> Self {
        match id {
            1235 => Self::USING_Identifier_BaseStringConstant,
            1236 => Self::USING_Identifier_EXPRESSION,
            1237 => Self::USING_Identifier_FUNCTION_BaseStringConstant,
            1238 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FullTypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1239 => Self::FullTypeExpr_AMPER_FullTypeExpr,
            1240 => Self::FullTypeExpr_PIPE_FullTypeExpr,
            1241 => Self::LPAREN_FullTypeExpr_RPAREN,
            1242 => Self::TYPEOF_Expr,
            1243 => Self::TypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncApplication {
    fn from_id(id: usize) -> Self {
        match id {
            1244 => Self::NodeName_LPAREN_OptFuncArgList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1245 => Self::FuncArgListInner,
            1246 => Self::FuncArgListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncArgListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1247 => Self::FuncArgListInner_COMMA_FuncCallArg,
            1248 => Self::FuncCallArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncCallArg {
    fn from_id(id: usize) -> Self {
        match id {
            1249 => Self::FuncCallArgExpr_OptFilterClause_OptSortClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncCallArgExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1250 => Self::AnyIdentifier_ASSIGN_Expr,
            1251 => Self::Expr,
            1252 => Self::PARAMETER_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArg {
    fn from_id(id: usize) -> Self {
        match id {
            1253 => Self::OptParameterKind_FuncDeclArgName_OptDefault,
            1254 => Self::OptParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1255 => Self::FuncDeclArgListInner,
            1256 => Self::FuncDeclArgListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1257 => Self::FuncDeclArg,
            1258 => Self::FuncDeclArgListInner_COMMA_FuncDeclArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgName {
    fn from_id(id: usize) -> Self {
        match id {
            1259 => Self::Identifier,
            1260 => Self::PARAMETER,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgs {
    fn from_id(id: usize) -> Self {
        match id {
            1261 => Self::FuncDeclArgList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1262 => Self::FuncApplication,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FunctionDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1263 => Self::FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FunctionDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1264 => Self::FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSingleSDLCommandBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FunctionType {
    fn from_id(id: usize) -> Self {
        match id {
            1265 => Self::FullTypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FutureRequirementDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1266 => Self::USING_FUTURE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FutureStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1267 => Self::CreateFutureStmt,
            1268 => Self::DropFutureStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GlobalDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1269 => Self::GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
            1270 => Self::PtrQuals_GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GlobalDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1271 => Self::GLOBAL_NodeName_ASSIGN_Expr,
            1272 => Self::PtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
            1273 => Self::GLOBAL_NodeName_PtrTarget,
            1274 => Self::PtrQuals_GLOBAL_NodeName_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingAtom {
    fn from_id(id: usize) -> Self {
        match id {
            1275 => Self::GroupingIdent,
            1276 => Self::LPAREN_GroupingIdentList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingAtomList {
    fn from_id(id: usize) -> Self {
        match id {
            1277 => Self::GroupingAtomListInner,
            1278 => Self::GroupingAtomListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingAtomListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1279 => Self::GroupingAtom,
            1280 => Self::GroupingAtomListInner_COMMA_GroupingAtom,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingElement {
    fn from_id(id: usize) -> Self {
        match id {
            1281 => Self::CUBE_LPAREN_GroupingAtomList_RPAREN,
            1282 => Self::GroupingAtom,
            1283 => Self::LBRACE_GroupingElementList_RBRACE,
            1284 => Self::ROLLUP_LPAREN_GroupingAtomList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingElementList {
    fn from_id(id: usize) -> Self {
        match id {
            1285 => Self::GroupingElementListInner,
            1286 => Self::GroupingElementListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingElementListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1287 => Self::GroupingElement,
            1288 => Self::GroupingElementListInner_COMMA_GroupingElement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingIdent {
    fn from_id(id: usize) -> Self {
        match id {
            1289 => Self::AT_Identifier,
            1290 => Self::DOT_Identifier,
            1291 => Self::Identifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingIdentList {
    fn from_id(id: usize) -> Self {
        match id {
            1292 => Self::GroupingIdent,
            1293 => Self::GroupingIdentList_COMMA_GroupingIdent,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Identifier {
    fn from_id(id: usize) -> Self {
        match id {
            1294 => Self::IDENT,
            1295 => Self::UnreservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IfThenElseExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1296 => Self::IF_Expr_THEN_Expr_ELSE_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexArg {
    fn from_id(id: usize) -> Self {
        match id {
            1297 => Self::AnyIdentifier_ASSIGN_Expr,
            1298 => Self::FuncDeclArgName_OptDefault,
            1299 => Self::FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
            1300 => Self::ParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1301 => Self::IndexArgListInner,
            1302 => Self::IndexArgListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexArgListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1303 => Self::IndexArg,
            1304 => Self::IndexArgListInner_COMMA_IndexArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1305 => Self::ABSTRACT_INDEX_NodeName_OptExtendingSimple_CreateIndexSDLCommandsBlock,
            1306 => Self::ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_CreateIndexSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1307 => Self::ABSTRACT_INDEX_NodeName_OptExtendingSimple,
            1308 => Self::ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexExtArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1309 => Self::LPAREN_OptIndexArgList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndirectionEl {
    fn from_id(id: usize) -> Self {
        match id {
            1310 => Self::LBRACKET_COLON_Expr_RBRACKET,
            1311 => Self::LBRACKET_Expr_COLON_Expr_RBRACKET,
            1312 => Self::LBRACKET_Expr_COLON_RBRACKET,
            1313 => Self::LBRACKET_Expr_RBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::InnerDDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1314 => Self::AlterAliasStmt,
            1315 => Self::AlterAnnotationStmt,
            1316 => Self::AlterCastStmt,
            1317 => Self::AlterConstraintStmt,
            1318 => Self::AlterFunctionStmt,
            1319 => Self::AlterGlobalStmt,
            1320 => Self::AlterIndexStmt,
            1321 => Self::AlterLinkStmt,
            1322 => Self::AlterModuleStmt,
            1323 => Self::AlterObjectTypeStmt,
            1324 => Self::AlterOperatorStmt,
            1325 => Self::AlterPropertyStmt,
            1326 => Self::AlterScalarTypeStmt,
            1327 => Self::CreateAliasStmt,
            1328 => Self::CreateAnnotationStmt,
            1329 => Self::CreateCastStmt,
            1330 => Self::CreateConstraintStmt,
            1331 => Self::CreateFunctionStmt,
            1332 => Self::CreateGlobalStmt,
            1333 => Self::CreateIndexMatchStmt,
            1334 => Self::CreateIndexStmt,
            1335 => Self::CreateLinkStmt,
            1336 => Self::CreateModuleStmt,
            1337 => Self::CreateObjectTypeStmt,
            1338 => Self::CreateOperatorStmt,
            1339 => Self::CreatePropertyStmt,
            1340 => Self::CreatePseudoTypeStmt,
            1341 => Self::CreateScalarTypeStmt,
            1342 => Self::DropAliasStmt,
            1343 => Self::DropAnnotationStmt,
            1344 => Self::DropCastStmt,
            1345 => Self::DropConstraintStmt,
            1346 => Self::DropFunctionStmt,
            1347 => Self::DropGlobalStmt,
            1348 => Self::DropIndexMatchStmt,
            1349 => Self::DropIndexStmt,
            1350 => Self::DropLinkStmt,
            1351 => Self::DropModuleStmt,
            1352 => Self::DropObjectTypeStmt,
            1353 => Self::DropOperatorStmt,
            1354 => Self::DropPropertyStmt,
            1355 => Self::DropScalarTypeStmt,
            1356 => Self::ExtensionStmt,
            1357 => Self::FutureStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::InternalGroup {
    fn from_id(id: usize) -> Self {
        match id {
            1358 => Self::FOR_GROUP_OptionallyAliasedExpr_UsingClause_ByClause_IN_Identifier_OptGroupingAlias_UNION_OptionallyAliasedExpr_OptFilterClause_OptSortClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::LimitClause {
    fn from_id(id: usize) -> Self {
        match id {
            1359 => Self::LIMIT_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::LinkDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1360 => Self::ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_CreateLinkSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::LinkDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1361 => Self::ABSTRACT_LINK_PtrNodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::MigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1362 => Self::AbortMigrationStmt,
            1363 => Self::AlterCurrentMigrationStmt,
            1364 => Self::AlterMigrationStmt,
            1365 => Self::CommitMigrationStmt,
            1366 => Self::CreateMigrationStmt,
            1367 => Self::DropMigrationStmt,
            1368 => Self::PopulateMigrationStmt,
            1369 => Self::ResetSchemaStmt,
            1370 => Self::StartMigrationStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ModuleDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1371 => Self::MODULE_ModuleName_SDLCommandBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ModuleName {
    fn from_id(id: usize) -> Self {
        match id {
            1372 => Self::DotName,
            1373 => Self::ModuleName_DOUBLECOLON_DotName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTuple {
    fn from_id(id: usize) -> Self {
        match id {
            1374 => Self::LPAREN_NamedTupleElementList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTupleElement {
    fn from_id(id: usize) -> Self {
        match id {
            1375 => Self::ShortNodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTupleElementList {
    fn from_id(id: usize) -> Self {
        match id {
            1376 => Self::NamedTupleElementListInner,
            1377 => Self::NamedTupleElementListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTupleElementListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1378 => Self::NamedTupleElement,
            1379 => Self::NamedTupleElementListInner_COMMA_NamedTupleElement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NestedQLBlockStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1380 => Self::OptWithDDLStmt,
            1381 => Self::SetFieldStmt,
            1382 => Self::Stmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1383 => Self::BaseName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NontrivialTypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1385 => Self::LPAREN_FullTypeExpr_RPAREN,
            1386 => Self::TYPEOF_Expr,
            1387 => Self::TypeExpr_AMPER_TypeExpr,
            1388 => Self::TypeExpr_PIPE_TypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ObjectTypeDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1389 => Self::ABSTRACT_TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
            1390 => Self::TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ObjectTypeDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1391 => Self::ABSTRACT_TYPE_NodeName_OptExtendingSimple,
            1392 => Self::TYPE_NodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OffsetClause {
    fn from_id(id: usize) -> Self {
        match id {
            1393 => Self::OFFSET_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1394 => Self::ON_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnSourceDeleteResetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1395 => Self::RESET_ON_SOURCE_DELETE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnSourceDeleteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1396 => Self::ON_SOURCE_DELETE_ALLOW,
            1397 => Self::ON_SOURCE_DELETE_DELETE_TARGET,
            1398 => Self::ON_SOURCE_DELETE_DELETE_TARGET_IF_ORPHAN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnTargetDeleteResetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1399 => Self::RESET_ON_TARGET_DELETE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnTargetDeleteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1400 => Self::ON_TARGET_DELETE_ALLOW,
            1401 => Self::ON_TARGET_DELETE_DEFERRED_RESTRICT,
            1402 => Self::ON_TARGET_DELETE_DELETE_SOURCE,
            1403 => Self::ON_TARGET_DELETE_RESTRICT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OperatorCode {
    fn from_id(id: usize) -> Self {
        match id {
            1404 => Self::USING_Identifier_BaseStringConstant,
            1405 => Self::USING_Identifier_EXPRESSION,
            1406 => Self::USING_Identifier_FUNCTION_BaseStringConstant,
            1407 => Self::USING_Identifier_OPERATOR_BaseStringConstant,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OperatorKind {
    fn from_id(id: usize) -> Self {
        match id {
            1408 => Self::INFIX,
            1409 => Self::POSTFIX,
            1410 => Self::PREFIX,
            1411 => Self::TERNARY,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptAlterUsingClause {
    fn from_id(id: usize) -> Self {
        match id {
            1412 => Self::USING_ParenExpr,
            1413 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptAnySubShape {
    fn from_id(id: usize) -> Self {
        match id {
            1414 => Self::COLON_Shape,
            1415 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptConcreteConstraintArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1416 => Self::LPAREN_OptPosCallArgList_RPAREN,
            1417 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateAccessPolicyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1418 => Self::CreateAccessPolicyCommandsBlock,
            1419 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateAnnotationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1420 => Self::CreateAnnotationCommandsBlock,
            1421 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1422 => Self::CreateCommandsBlock,
            1423 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1424 => Self::CreateConcreteLinkCommandsBlock,
            1425 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateConcretePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1426 => Self::CreateConcretePropertyCommandsBlock,
            1427 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateDatabaseCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1428 => Self::CreateDatabaseCommandsBlock,
            1429 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateExtensionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1430 => Self::CreateExtensionCommandsBlock,
            1431 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateExtensionPackageCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1432 => Self::CreateExtensionPackageCommandsBlock,
            1433 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateGlobalCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1434 => Self::CreateGlobalCommandsBlock,
            1435 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1436 => Self::CreateIndexCommandsBlock,
            1437 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateIndexMatchCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1438 => Self::CreateIndexMatchCommandsBlock,
            1439 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1440 => Self::CreateLinkCommandsBlock,
            1441 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateMigrationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1442 => Self::CreateMigrationCommandsBlock,
            1443 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1444 => Self::CreateObjectTypeCommandsBlock,
            1445 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateOperatorCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1446 => Self::CreateOperatorCommandsBlock,
            1447 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreatePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1448 => Self::CreatePropertyCommandsBlock,
            1449 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreatePseudoTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1450 => Self::CreatePseudoTypeCommandsBlock,
            1451 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateRewriteCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1452 => Self::CreateRewriteCommandsBlock,
            1453 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateRoleCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1454 => Self::CreateRoleCommandsBlock,
            1455 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateScalarTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1456 => Self::CreateScalarTypeCommandsBlock,
            1457 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateTriggerCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1458 => Self::CreateTriggerCommandsBlock,
            1459 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDefault {
    fn from_id(id: usize) -> Self {
        match id {
            1460 => Self::EQUALS_Expr,
            1461 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDeferred {
    fn from_id(id: usize) -> Self {
        match id {
            1462 => Self::DEFERRED,
            1463 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDelegated {
    fn from_id(id: usize) -> Self {
        match id {
            1464 => Self::DELEGATED,
            1465 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDirection {
    fn from_id(id: usize) -> Self {
        match id {
            1466 => Self::ASC,
            1467 => Self::DESC,
            1468 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropConcreteIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1469 => Self::DropConcreteIndexCommandsBlock,
            1470 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1471 => Self::DropConcreteLinkCommandsBlock,
            1472 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1473 => Self::DropLinkCommandsBlock,
            1474 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1475 => Self::DropObjectTypeCommandsBlock,
            1476 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExceptExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1477 => Self::EXCEPT_ParenExpr,
            1478 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExprList {
    fn from_id(id: usize) -> Self {
        match id {
            1479 => Self::ExprList,
            1480 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1481 => Self::Extending,
            1482 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExtendingSimple {
    fn from_id(id: usize) -> Self {
        match id {
            1483 => Self::ExtendingSimple,
            1484 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExtensionVersion {
    fn from_id(id: usize) -> Self {
        match id {
            1485 => Self::ExtensionVersion,
            1486 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptFilterClause {
    fn from_id(id: usize) -> Self {
        match id {
            1487 => Self::FilterClause,
            1488 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptFuncArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1489 => Self::FuncArgList,
            1490 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptGroupingAlias {
    fn from_id(id: usize) -> Self {
        match id {
            1491 => Self::COMMA_Identifier,
            1492 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptIfNotExists {
    fn from_id(id: usize) -> Self {
        match id {
            1493 => Self::IF_NOT_EXISTS,
            1494 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptIndexArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1495 => Self::IndexArgList,
            1496 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptIndexExtArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1497 => Self::IndexExtArgList,
            1498 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptMigrationNameParentName {
    fn from_id(id: usize) -> Self {
        match id {
            1499 => Self::ShortNodeName,
            1500 => Self::ShortNodeName_ONTO_ShortNodeName,
            1501 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptNonesOrder {
    fn from_id(id: usize) -> Self {
        match id {
            1502 => Self::EMPTY_FIRST,
            1503 => Self::EMPTY_LAST,
            1504 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptOnExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1505 => Self::OnExpr,
            1506 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptParameterKind {
    fn from_id(id: usize) -> Self {
        match id {
            1507 => Self::ParameterKind,
            1508 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPosCallArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1509 => Self::PosCallArgList,
            1510 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPosition {
    fn from_id(id: usize) -> Self {
        match id {
            1511 => Self::AFTER_NodeName,
            1512 => Self::BEFORE_NodeName,
            1513 => Self::FIRST,
            1514 => Self::LAST,
            1515 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPtrQuals {
    fn from_id(id: usize) -> Self {
        match id {
            1516 => Self::PtrQuals,
            1517 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPtrTarget {
    fn from_id(id: usize) -> Self {
        match id {
            1518 => Self::PtrTarget,
            1519 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSelectLimit {
    fn from_id(id: usize) -> Self {
        match id {
            1520 => Self::SelectLimit,
            1521 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSemicolons {
    fn from_id(id: usize) -> Self {
        match id {
            1522 => Self::Semicolons,
            1523 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptShortExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1524 => Self::ShortExtending,
            1525 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSortClause {
    fn from_id(id: usize) -> Self {
        match id {
            1526 => Self::SortClause,
            1527 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSuperuser {
    fn from_id(id: usize) -> Self {
        match id {
            1528 => Self::SUPERUSER,
            1529 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptTransactionModeList {
    fn from_id(id: usize) -> Self {
        match id {
            1530 => Self::TransactionModeList,
            1531 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptTypeIntersection {
    fn from_id(id: usize) -> Self {
        match id {
            1532 => Self::TypeIntersection,
            1533 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptTypeQualifier {
    fn from_id(id: usize) -> Self {
        match id {
            1534 => Self::OPTIONAL,
            1535 => Self::SET_OF,
            1536 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptUnlessConflictClause {
    fn from_id(id: usize) -> Self {
        match id {
            1537 => Self::UnlessConflictCause,
            1538 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptUsingBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1539 => Self::USING_ParenExpr,
            1540 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptUsingClause {
    fn from_id(id: usize) -> Self {
        match id {
            1541 => Self::UsingClause,
            1542 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptWhenBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1543 => Self::WHEN_ParenExpr,
            1544 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptWithDDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1545 => Self::DDLWithBlock_WithDDLStmt,
            1546 => Self::WithDDLStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptionalOptional {
    fn from_id(id: usize) -> Self {
        match id {
            1547 => Self::OPTIONAL,
            1548 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptionallyAliasedExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1549 => Self::AliasedExpr,
            1550 => Self::Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OrderbyExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1551 => Self::Expr_OptDirection_OptNonesOrder,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OrderbyList {
    fn from_id(id: usize) -> Self {
        match id {
            1552 => Self::OrderbyExpr,
            1553 => Self::OrderbyList_THEN_OrderbyExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ParameterKind {
    fn from_id(id: usize) -> Self {
        match id {
            1554 => Self::NAMEDONLY,
            1555 => Self::VARIADIC,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ParenExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1556 => Self::LPAREN_ExprStmt_RPAREN,
            1557 => Self::LPAREN_Expr_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ParenTypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1558 => Self::LPAREN_FullTypeExpr_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PartialReservedKeyword {
    fn from_id(id: usize) -> Self {
        match id {
            1559 => Self::EXCEPT,
            1560 => Self::INTERSECT,
            1561 => Self::UNION,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Path {
    fn from_id(id: usize) -> Self {
        match id {
            1562 => Self::Expr_PathStep_P_DOT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PathNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1563 => Self::PtrIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PathStep {
    fn from_id(id: usize) -> Self {
        match id {
            1564 => Self::AT_PathNodeName,
            1565 => Self::DOTBW_PathStepName,
            1566 => Self::DOT_ICONST,
            1567 => Self::DOT_PathStepName,
            1568 => Self::TypeIntersection,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PathStepName {
    fn from_id(id: usize) -> Self {
        match id {
            1569 => Self::DUNDERTYPE,
            1570 => Self::PathNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PointerName {
    fn from_id(id: usize) -> Self {
        match id {
            1571 => Self::DUNDERTYPE,
            1572 => Self::PtrNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PopulateMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1573 => Self::POPULATE_MIGRATION,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PosCallArg {
    fn from_id(id: usize) -> Self {
        match id {
            1574 => Self::Expr_OptFilterClause_OptSortClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PosCallArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1575 => Self::PosCallArg,
            1576 => Self::PosCallArgList_COMMA_PosCallArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PropertyDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1577 => Self::ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_CreatePropertySDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PropertyDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1578 => Self::ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrIdentifier {
    fn from_id(id: usize) -> Self {
        match id {
            1579 => Self::Identifier,
            1580 => Self::PartialReservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrName {
    fn from_id(id: usize) -> Self {
        match id {
            1581 => Self::PtrIdentifier,
            1582 => Self::QualifiedName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1583 => Self::PtrName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrQualifiedNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1584 => Self::QualifiedName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrQuals {
    fn from_id(id: usize) -> Self {
        match id {
            1585 => Self::MULTI,
            1586 => Self::OPTIONAL,
            1587 => Self::OPTIONAL_MULTI,
            1588 => Self::OPTIONAL_SINGLE,
            1589 => Self::REQUIRED,
            1590 => Self::REQUIRED_MULTI,
            1591 => Self::REQUIRED_SINGLE,
            1592 => Self::SINGLE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrTarget {
    fn from_id(id: usize) -> Self {
        match id {
            1593 => Self::ARROW_FullTypeExpr,
            1594 => Self::COLON_FullTypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::QualifiedName {
    fn from_id(id: usize) -> Self {
        match id {
            1595 => Self::DUNDERSTD_DOUBLECOLON_ColonedIdents,
            1596 => Self::Identifier_DOUBLECOLON_ColonedIdents,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RenameStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1597 => Self::RENAME_TO_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ReservedKeyword {
    fn from_id(id: usize) -> Self {
        match id {
            1598 => Self::ADMINISTER,
            1599 => Self::ALTER,
            1600 => Self::ANALYZE,
            1601 => Self::AND,
            1602 => Self::ANYARRAY,
            1603 => Self::ANYOBJECT,
            1604 => Self::ANYTUPLE,
            1605 => Self::ANYTYPE,
            1606 => Self::BEGIN,
            1607 => Self::BY,
            1608 => Self::CASE,
            1609 => Self::CHECK,
            1610 => Self::COMMIT,
            1611 => Self::CONFIGURE,
            1612 => Self::CREATE,
            1613 => Self::DEALLOCATE,
            1614 => Self::DELETE,
            1615 => Self::DESCRIBE,
            1616 => Self::DETACHED,
            1617 => Self::DISCARD,
            1618 => Self::DISTINCT,
            1619 => Self::DO,
            1620 => Self::DROP,
            1621 => Self::DUNDERDEFAULT,
            1622 => Self::DUNDEREDGEDBSYS,
            1623 => Self::DUNDEREDGEDBTPL,
            1624 => Self::DUNDERNEW,
            1625 => Self::DUNDEROLD,
            1626 => Self::DUNDERSOURCE,
            1627 => Self::DUNDERSPECIFIED,
            1628 => Self::DUNDERSTD,
            1629 => Self::DUNDERSUBJECT,
            1630 => Self::DUNDERTYPE,
            1631 => Self::ELSE,
            1632 => Self::END,
            1633 => Self::EXISTS,
            1634 => Self::EXPLAIN,
            1635 => Self::EXTENDING,
            1636 => Self::FALSE,
            1637 => Self::FETCH,
            1638 => Self::FILTER,
            1639 => Self::FOR,
            1640 => Self::GET,
            1641 => Self::GLOBAL,
            1642 => Self::GRANT,
            1643 => Self::GROUP,
            1644 => Self::IF,
            1645 => Self::ILIKE,
            1646 => Self::IMPORT,
            1647 => Self::IN,
            1648 => Self::INSERT,
            1649 => Self::INTROSPECT,
            1650 => Self::IS,
            1651 => Self::LIKE,
            1652 => Self::LIMIT,
            1653 => Self::LISTEN,
            1654 => Self::LOAD,
            1655 => Self::LOCK,
            1656 => Self::MATCH,
            1657 => Self::MODULE,
            1658 => Self::MOVE,
            1659 => Self::NEVER,
            1660 => Self::NOT,
            1661 => Self::NOTIFY,
            1662 => Self::OFFSET,
            1663 => Self::ON,
            1664 => Self::OPTIONAL,
            1665 => Self::OR,
            1666 => Self::OVER,
            1667 => Self::PARTITION,
            1668 => Self::PREPARE,
            1669 => Self::RAISE,
            1670 => Self::REFRESH,
            1671 => Self::REVOKE,
            1672 => Self::ROLLBACK,
            1673 => Self::SELECT,
            1674 => Self::SET,
            1675 => Self::SINGLE,
            1676 => Self::START,
            1677 => Self::TRUE,
            1678 => Self::TYPEOF,
            1679 => Self::UPDATE,
            1680 => Self::VARIADIC,
            1681 => Self::WHEN,
            1682 => Self::WINDOW,
            1683 => Self::WITH,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ResetFieldStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1684 => Self::RESET_DEFAULT,
            1685 => Self::RESET_IDENT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ResetSchemaStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1686 => Self::RESET_SCHEMA_TO_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ResetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1687 => Self::RESET_ALIAS_Identifier,
            1688 => Self::RESET_ALIAS_STAR,
            1689 => Self::RESET_MODULE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1690 => Self::REWRITE_RewriteKindList_USING_ParenExpr_CreateRewriteSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1691 => Self::REWRITE_RewriteKindList_USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteKind {
    fn from_id(id: usize) -> Self {
        match id {
            1692 => Self::INSERT,
            1693 => Self::UPDATE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteKindList {
    fn from_id(id: usize) -> Self {
        match id {
            1694 => Self::RewriteKind,
            1695 => Self::RewriteKindList_COMMA_RewriteKind,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1696 => Self::AlterRoleStmt,
            1697 => Self::CreateRoleStmt,
            1698 => Self::DropRoleStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLBlockStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1699 => Self::AliasDeclaration,
            1700 => Self::AnnotationDeclaration,
            1701 => Self::ConstraintDeclaration,
            1702 => Self::FunctionDeclaration,
            1703 => Self::GlobalDeclaration,
            1704 => Self::IndexDeclaration,
            1705 => Self::LinkDeclaration,
            1706 => Self::ModuleDeclaration,
            1707 => Self::ObjectTypeDeclaration,
            1708 => Self::PropertyDeclaration,
            1709 => Self::ScalarTypeDeclaration,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1710 => Self::LBRACE_OptSemicolons_RBRACE,
            1711 => Self::LBRACE_OptSemicolons_SDLStatements_RBRACE,
            1712 => Self::LBRACE_OptSemicolons_SDLShortStatement_RBRACE,
            1713 => Self::LBRACE_OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement_RBRACE,
            1714 => Self::LBRACE_OptSemicolons_SDLStatements_Semicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLDocument {
    fn from_id(id: usize) -> Self {
        match id {
            1715 => Self::OptSemicolons,
            1716 => Self::OptSemicolons_SDLStatements,
            1717 => Self::OptSemicolons_SDLStatements_Semicolons,
            1718 => Self::OptSemicolons_SDLShortStatement,
            1719 => Self::OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLShortStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1720 => Self::AliasDeclarationShort,
            1721 => Self::AnnotationDeclarationShort,
            1722 => Self::ConstraintDeclarationShort,
            1723 => Self::ExtensionRequirementDeclaration,
            1724 => Self::FunctionDeclarationShort,
            1725 => Self::FutureRequirementDeclaration,
            1726 => Self::GlobalDeclarationShort,
            1727 => Self::IndexDeclarationShort,
            1728 => Self::LinkDeclarationShort,
            1729 => Self::ObjectTypeDeclarationShort,
            1730 => Self::PropertyDeclarationShort,
            1731 => Self::ScalarTypeDeclarationShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1732 => Self::SDLBlockStatement,
            1733 => Self::SDLShortStatement_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLStatements {
    fn from_id(id: usize) -> Self {
        match id {
            1734 => Self::SDLStatement,
            1735 => Self::SDLStatements_OptSemicolons_SDLStatement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ScalarTypeDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1736 => Self::ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
            1737 => Self::SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ScalarTypeDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1738 => Self::ABSTRACT_SCALAR_TYPE_NodeName_OptExtending,
            1739 => Self::SCALAR_TYPE_NodeName_OptExtending,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SchemaItem {
    fn from_id(id: usize) -> Self {
        match id {
            1740 => Self::SchemaObjectClass_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SchemaObjectClass {
    fn from_id(id: usize) -> Self {
        match id {
            1741 => Self::ALIAS,
            1742 => Self::ANNOTATION,
            1743 => Self::CAST,
            1744 => Self::CONSTRAINT,
            1745 => Self::FUNCTION,
            1746 => Self::LINK,
            1747 => Self::MODULE,
            1748 => Self::OPERATOR,
            1749 => Self::PROPERTY,
            1750 => Self::SCALAR_TYPE,
            1751 => Self::TYPE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SelectLimit {
    fn from_id(id: usize) -> Self {
        match id {
            1752 => Self::LimitClause,
            1753 => Self::OffsetClause,
            1754 => Self::OffsetClause_LimitClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Semicolons {
    fn from_id(id: usize) -> Self {
        match id {
            1755 => Self::SEMICOLON,
            1756 => Self::Semicolons_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SessionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1757 => Self::ResetStmt,
            1758 => Self::SetStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Set {
    fn from_id(id: usize) -> Self {
        match id {
            1759 => Self::LBRACE_OptExprList_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetAnnotation {
    fn from_id(id: usize) -> Self {
        match id {
            1760 => Self::ANNOTATION_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetCardinalityStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1761 => Self::RESET_CARDINALITY_OptAlterUsingClause,
            1762 => Self::SET_MULTI,
            1763 => Self::SET_SINGLE_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetDelegatedStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1764 => Self::RESET_DELEGATED,
            1765 => Self::SET_DELEGATED,
            1766 => Self::SET_NOT_DELEGATED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetField {
    fn from_id(id: usize) -> Self {
        match id {
            1767 => Self::Identifier_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetFieldStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1768 => Self::SET_Identifier_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetGlobalTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1769 => Self::RESET_TYPE,
            1770 => Self::SETTYPE_FullTypeExpr_OptAlterUsingClause,
            1771 => Self::SETTYPE_FullTypeExpr_RESET_TO_DEFAULT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetPointerTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1772 => Self::RESET_TYPE,
            1773 => Self::SETTYPE_FullTypeExpr_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetRequiredInCreateStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1774 => Self::SET_REQUIRED_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetRequiredStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1775 => Self::DROP_REQUIRED,
            1776 => Self::RESET_OPTIONALITY,
            1777 => Self::SET_OPTIONAL,
            1778 => Self::SET_REQUIRED_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1779 => Self::SET_ALIAS_Identifier_AS_MODULE_ModuleName,
            1780 => Self::SET_MODULE_ModuleName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Shape {
    fn from_id(id: usize) -> Self {
        match id {
            1781 => Self::LBRACE_RBRACE,
            1782 => Self::LBRACE_ShapeElementList_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapeElement {
    fn from_id(id: usize) -> Self {
        match id {
            1783 => Self::ComputableShapePointer,
            1784 => Self::ShapePointer_OptAnySubShape_OptFilterClause_OptSortClause_OptSelectLimit,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapeElementList {
    fn from_id(id: usize) -> Self {
        match id {
            1785 => Self::ShapeElementListInner,
            1786 => Self::ShapeElementListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapeElementListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1787 => Self::ShapeElement,
            1788 => Self::ShapeElementListInner_COMMA_ShapeElement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapePath {
    fn from_id(id: usize) -> Self {
        match id {
            1789 => Self::AT_PathNodeName,
            1790 => Self::PathStepName_OptTypeIntersection,
            1791 => Self::Splat,
            1792 => Self::TypeIntersection_DOT_PathStepName_OptTypeIntersection,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1793 => Self::ShapePath,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShortExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1794 => Self::EXTENDING_ShortNodeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShortNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1795 => Self::Identifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShortNodeNameList {
    fn from_id(id: usize) -> Self {
        match id {
            1796 => Self::ShortNodeName,
            1797 => Self::ShortNodeNameList_COMMA_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleDelete {
    fn from_id(id: usize) -> Self {
        match id {
            1798 => Self::DELETE_Expr_OptFilterClause_OptSortClause_OptSelectLimit,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleFor {
    fn from_id(id: usize) -> Self {
        match id {
            1799 => Self::FOR_OptionalOptional_Identifier_IN_AtomicExpr_UNION_Expr,
            1800 => Self::FOR_OptionalOptional_Identifier_IN_AtomicExpr_ExprStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleGroup {
    fn from_id(id: usize) -> Self {
        match id {
            1801 => Self::GROUP_OptionallyAliasedExpr_OptUsingClause_ByClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleInsert {
    fn from_id(id: usize) -> Self {
        match id {
            1802 => Self::INSERT_Expr_OptUnlessConflictClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleSelect {
    fn from_id(id: usize) -> Self {
        match id {
            1803 => Self::SELECT_OptionallyAliasedExpr_OptFilterClause_OptSortClause_OptSelectLimit,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleShapePath {
    fn from_id(id: usize) -> Self {
        match id {
            1804 => Self::AT_PathNodeName,
            1805 => Self::PathStepName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1806 => Self::SimpleShapePath,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleTypeName {
    fn from_id(id: usize) -> Self {
        match id {
            1807 => Self::ANYOBJECT,
            1808 => Self::ANYTUPLE,
            1809 => Self::ANYTYPE,
            1810 => Self::PtrNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleTypeNameList {
    fn from_id(id: usize) -> Self {
        match id {
            1811 => Self::SimpleTypeName,
            1812 => Self::SimpleTypeNameList_COMMA_SimpleTypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleUpdate {
    fn from_id(id: usize) -> Self {
        match id {
            1813 => Self::UPDATE_Expr_OptFilterClause_SET_Shape,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SingleStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1814 => Self::ConfigStmt,
            1815 => Self::DDLStmt,
            1816 => Self::IfThenElseExpr,
            1817 => Self::SessionStmt,
            1818 => Self::Stmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SortClause {
    fn from_id(id: usize) -> Self {
        match id {
            1819 => Self::ORDERBY_OrderbyList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Splat {
    fn from_id(id: usize) -> Self {
        match id {
            1820 => Self::DOUBLESTAR,
            1821 => Self::ParenTypeExpr_DOT_DOUBLESTAR,
            1822 => Self::ParenTypeExpr_DOT_STAR,
            1823 => Self::ParenTypeExpr_TypeIntersection_DOT_DOUBLESTAR,
            1824 => Self::ParenTypeExpr_TypeIntersection_DOT_STAR,
            1825 => Self::PathStepName_DOT_DOUBLESTAR,
            1826 => Self::PathStepName_DOT_STAR,
            1827 => Self::PathStepName_TypeIntersection_DOT_DOUBLESTAR,
            1828 => Self::PathStepName_TypeIntersection_DOT_STAR,
            1829 => Self::PtrQualifiedNodeName_DOT_DOUBLESTAR,
            1830 => Self::PtrQualifiedNodeName_DOT_STAR,
            1831 => Self::PtrQualifiedNodeName_TypeIntersection_DOT_DOUBLESTAR,
            1832 => Self::PtrQualifiedNodeName_TypeIntersection_DOT_STAR,
            1833 => Self::STAR,
            1834 => Self::TypeIntersection_DOT_DOUBLESTAR,
            1835 => Self::TypeIntersection_DOT_STAR,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::StartMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1836 => Self::START_MIGRATION_TO_SDLCommandBlock,
            1837 => Self::START_MIGRATION_REWRITE,
            1838 => Self::START_MIGRATION_TO_COMMITTED_SCHEMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::StatementBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1839 => Self::SingleStatement,
            1840 => Self::StatementBlock_Semicolons_SingleStatement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Stmt {
    fn from_id(id: usize) -> Self {
        match id {
            1841 => Self::AdministerStmt,
            1842 => Self::AnalyzeStmt,
            1843 => Self::DescribeStmt,
            1844 => Self::ExprStmt,
            1845 => Self::TransactionStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Subtype {
    fn from_id(id: usize) -> Self {
        match id {
            1846 => Self::BaseNumberConstant,
            1847 => Self::BaseStringConstant,
            1848 => Self::FullTypeExpr,
            1849 => Self::Identifier_COLON_FullTypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SubtypeList {
    fn from_id(id: usize) -> Self {
        match id {
            1850 => Self::SubtypeListInner,
            1851 => Self::SubtypeListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SubtypeListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1852 => Self::Subtype,
            1853 => Self::SubtypeListInner_COMMA_Subtype,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TransactionMode {
    fn from_id(id: usize) -> Self {
        match id {
            1854 => Self::DEFERRABLE,
            1855 => Self::ISOLATION_SERIALIZABLE,
            1856 => Self::NOT_DEFERRABLE,
            1857 => Self::READ_ONLY,
            1858 => Self::READ_WRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TransactionModeList {
    fn from_id(id: usize) -> Self {
        match id {
            1859 => Self::TransactionMode,
            1860 => Self::TransactionModeList_COMMA_TransactionMode,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TransactionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1861 => Self::COMMIT,
            1862 => Self::DECLARE_SAVEPOINT_Identifier,
            1863 => Self::RELEASE_SAVEPOINT_Identifier,
            1864 => Self::ROLLBACK,
            1865 => Self::ROLLBACK_TO_SAVEPOINT_Identifier,
            1866 => Self::START_TRANSACTION_OptTransactionModeList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1867 => Self::TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_CreateTriggerSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1868 => Self::TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerKind {
    fn from_id(id: usize) -> Self {
        match id {
            1869 => Self::DELETE,
            1870 => Self::INSERT,
            1871 => Self::UPDATE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerKindList {
    fn from_id(id: usize) -> Self {
        match id {
            1872 => Self::TriggerKind,
            1873 => Self::TriggerKindList_COMMA_TriggerKind,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerScope {
    fn from_id(id: usize) -> Self {
        match id {
            1874 => Self::ALL,
            1875 => Self::EACH,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerTiming {
    fn from_id(id: usize) -> Self {
        match id {
            1876 => Self::AFTER,
            1877 => Self::AFTER_COMMIT_OF,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Tuple {
    fn from_id(id: usize) -> Self {
        match id {
            1878 => Self::LPAREN_Expr_COMMA_OptExprList_RPAREN,
            1879 => Self::LPAREN_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1880 => Self::NontrivialTypeExpr,
            1881 => Self::SimpleTypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeIntersection {
    fn from_id(id: usize) -> Self {
        match id {
            1882 => Self::LBRACKET_IS_FullTypeExpr_RBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeName {
    fn from_id(id: usize) -> Self {
        match id {
            1883 => Self::CollectionTypeName,
            1884 => Self::SimpleTypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeNameList {
    fn from_id(id: usize) -> Self {
        match id {
            1885 => Self::TypeName,
            1886 => Self::TypeNameList_COMMA_TypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnlessConflictCause {
    fn from_id(id: usize) -> Self {
        match id {
            1887 => Self::UNLESS_CONFLICT_UnlessConflictSpecifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnlessConflictSpecifier {
    fn from_id(id: usize) -> Self {
        match id {
            1888 => Self::ON_Expr,
            1889 => Self::ON_Expr_ELSE_Expr,
            1890 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnqualifiedPointerName {
    fn from_id(id: usize) -> Self {
        match id {
            1891 => Self::PointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnreservedKeyword {
    fn from_id(id: usize) -> Self {
        match id {
            1892 => Self::ABORT,
            1893 => Self::ABSTRACT,
            1894 => Self::ACCESS,
            1895 => Self::AFTER,
            1896 => Self::ALIAS,
            1897 => Self::ALL,
            1898 => Self::ALLOW,
            1899 => Self::ANNOTATION,
            1900 => Self::APPLIED,
            1901 => Self::AS,
            1902 => Self::ASC,
            1903 => Self::ASSIGNMENT,
            1904 => Self::BEFORE,
            1905 => Self::BLOBAL,
            1906 => Self::BRANCH,
            1907 => Self::CARDINALITY,
            1908 => Self::CAST,
            1909 => Self::COMMITTED,
            1910 => Self::CONFIG,
            1911 => Self::CONFLICT,
            1912 => Self::CONSTRAINT,
            1913 => Self::CUBE,
            1914 => Self::CURRENT,
            1915 => Self::DATA,
            1916 => Self::DATABASE,
            1917 => Self::DDL,
            1918 => Self::DECLARE,
            1919 => Self::DEFAULT,
            1920 => Self::DEFERRABLE,
            1921 => Self::DEFERRED,
            1922 => Self::DELEGATED,
            1923 => Self::DENY,
            1924 => Self::DESC,
            1925 => Self::EACH,
            1926 => Self::EMPTY,
            1927 => Self::EXPRESSION,
            1928 => Self::EXTENSION,
            1929 => Self::FINAL,
            1930 => Self::FIRST,
            1931 => Self::FORCE,
            1932 => Self::FROM,
            1933 => Self::FUNCTION,
            1934 => Self::FUTURE,
            1935 => Self::IMPLICIT,
            1936 => Self::INDEX,
            1937 => Self::INFIX,
            1938 => Self::INHERITABLE,
            1939 => Self::INSTANCE,
            1940 => Self::INTO,
            1941 => Self::ISOLATION,
            1942 => Self::JSON,
            1943 => Self::LAST,
            1944 => Self::LINK,
            1945 => Self::MIGRATION,
            1946 => Self::MULTI,
            1947 => Self::NAMED,
            1948 => Self::OBJECT,
            1949 => Self::OF,
            1950 => Self::ONLY,
            1951 => Self::ONTO,
            1952 => Self::OPERATOR,
            1953 => Self::OPTIONALITY,
            1954 => Self::ORDER,
            1955 => Self::ORPHAN,
            1956 => Self::OVERLOADED,
            1957 => Self::OWNED,
            1958 => Self::PACKAGE,
            1959 => Self::POLICY,
            1960 => Self::POPULATE,
            1961 => Self::POSTFIX,
            1962 => Self::PREFIX,
            1963 => Self::PROPERTY,
            1964 => Self::PROPOSED,
            1965 => Self::PSEUDO,
            1966 => Self::READ,
            1967 => Self::REJECT,
            1968 => Self::RELEASE,
            1969 => Self::RENAME,
            1970 => Self::REQUIRED,
            1971 => Self::RESET,
            1972 => Self::RESTRICT,
            1973 => Self::REWRITE,
            1974 => Self::ROLE,
            1975 => Self::ROLES,
            1976 => Self::ROLLUP,
            1977 => Self::SAVEPOINT,
            1978 => Self::SCALAR,
            1979 => Self::SCHEMA,
            1980 => Self::SDL,
            1981 => Self::SERIALIZABLE,
            1982 => Self::SESSION,
            1983 => Self::SOURCE,
            1984 => Self::SUPERUSER,
            1985 => Self::SYSTEM,
            1986 => Self::TARGET,
            1987 => Self::TEMPLATE,
            1988 => Self::TERNARY,
            1989 => Self::TEXT,
            1990 => Self::THEN,
            1991 => Self::TO,
            1992 => Self::TRANSACTION,
            1993 => Self::TRIGGER,
            1994 => Self::TYPE,
            1995 => Self::UNLESS,
            1996 => Self::USING,
            1997 => Self::VERBOSE,
            1998 => Self::VERSION,
            1999 => Self::VIEW,
            2000 => Self::WRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Using {
    fn from_id(id: usize) -> Self {
        match id {
            2001 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UsingClause {
    fn from_id(id: usize) -> Self {
        match id {
            2002 => Self::USING_AliasedExprList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UsingStmt {
    fn from_id(id: usize) -> Self {
        match id {
            2003 => Self::RESET_EXPRESSION,
            2004 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithBlock {
    fn from_id(id: usize) -> Self {
        match id {
            2005 => Self::WITH_WithDeclList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            2006 => Self::InnerDDLStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDecl {
    fn from_id(id: usize) -> Self {
        match id {
            2007 => Self::AliasDecl,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDeclList {
    fn from_id(id: usize) -> Self {
        match id {
            2008 => Self::WithDeclListInner,
            2009 => Self::WithDeclListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDeclListInner {
    fn from_id(id: usize) -> Self {
        match id {
            2010 => Self::WithDecl,
            2011 => Self::WithDeclListInner_COMMA_WithDecl,
          _ => unreachable!(),
        }
    }
}
