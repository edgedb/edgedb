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

impl super::FromId for super::AlterFunctionCommand {
    fn from_id(id: usize) -> Self {
        match id {
            217 => Self::AlterAnnotationValueStmt,
            218 => Self::CreateAnnotationValueStmt,
            219 => Self::DropAnnotationValueStmt,
            220 => Self::FromFunction,
            221 => Self::RenameStmt,
            222 => Self::ResetFieldStmt,
            223 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            224 => Self::AlterFunctionCommand,
            225 => Self::LBRACE_AlterFunctionCommandsList_OptSemicolons_RBRACE,
            226 => Self::LBRACE_OptSemicolons_RBRACE,
            227 => Self::LBRACE_Semicolons_AlterFunctionCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            228 => Self::AlterFunctionCommand,
            229 => Self::AlterFunctionCommandsList_Semicolons_AlterFunctionCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterFunctionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            230 => Self::ALTER_FUNCTION_NodeName_CreateFunctionArgs_AlterFunctionCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalCommand {
    fn from_id(id: usize) -> Self {
        match id {
            231 => Self::AlterAnnotationValueStmt,
            232 => Self::CreateAnnotationValueStmt,
            233 => Self::DropAnnotationValueStmt,
            234 => Self::RenameStmt,
            235 => Self::ResetFieldStmt,
            236 => Self::SetCardinalityStmt,
            237 => Self::SetFieldStmt,
            238 => Self::SetGlobalTypeStmt,
            239 => Self::SetRequiredStmt,
            240 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            241 => Self::AlterGlobalCommand,
            242 => Self::LBRACE_AlterGlobalCommandsList_OptSemicolons_RBRACE,
            243 => Self::LBRACE_OptSemicolons_RBRACE,
            244 => Self::LBRACE_Semicolons_AlterGlobalCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            245 => Self::AlterGlobalCommand,
            246 => Self::AlterGlobalCommandsList_Semicolons_AlterGlobalCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterGlobalStmt {
    fn from_id(id: usize) -> Self {
        match id {
            247 => Self::ALTER_GLOBAL_NodeName_AlterGlobalCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            248 => Self::AlterAnnotationValueStmt,
            249 => Self::CreateAnnotationValueStmt,
            250 => Self::DropAnnotationValueStmt,
            251 => Self::RenameStmt,
            252 => Self::ResetFieldStmt,
            253 => Self::SetFieldStmt,
            254 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            255 => Self::AlterIndexCommand,
            256 => Self::LBRACE_AlterIndexCommandsList_OptSemicolons_RBRACE,
            257 => Self::LBRACE_OptSemicolons_RBRACE,
            258 => Self::LBRACE_Semicolons_AlterIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            259 => Self::AlterIndexCommand,
            260 => Self::AlterIndexCommandsList_Semicolons_AlterIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            261 => Self::ALTER_ABSTRACT_INDEX_NodeName_AlterIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            262 => Self::AlterAnnotationValueStmt,
            263 => Self::AlterConcreteConstraintStmt,
            264 => Self::AlterConcreteIndexStmt,
            265 => Self::AlterConcretePropertyStmt,
            266 => Self::AlterRewriteStmt,
            267 => Self::AlterSimpleExtending,
            268 => Self::CreateAnnotationValueStmt,
            269 => Self::CreateConcreteConstraintStmt,
            270 => Self::CreateConcreteIndexStmt,
            271 => Self::CreateConcretePropertyStmt,
            272 => Self::CreateRewriteStmt,
            273 => Self::DropAnnotationValueStmt,
            274 => Self::DropConcreteConstraintStmt,
            275 => Self::DropConcreteIndexStmt,
            276 => Self::DropConcretePropertyStmt,
            277 => Self::DropRewriteStmt,
            278 => Self::RenameStmt,
            279 => Self::ResetFieldStmt,
            280 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            281 => Self::AlterLinkCommand,
            282 => Self::LBRACE_AlterLinkCommandsList_OptSemicolons_RBRACE,
            283 => Self::LBRACE_OptSemicolons_RBRACE,
            284 => Self::LBRACE_Semicolons_AlterLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            285 => Self::AlterLinkCommand,
            286 => Self::AlterLinkCommandsList_Semicolons_AlterLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            287 => Self::ALTER_ABSTRACT_LINK_PtrNodeName_AlterLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            288 => Self::ResetFieldStmt,
            289 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            290 => Self::AlterMigrationCommand,
            291 => Self::LBRACE_AlterMigrationCommandsList_OptSemicolons_RBRACE,
            292 => Self::LBRACE_OptSemicolons_RBRACE,
            293 => Self::LBRACE_Semicolons_AlterMigrationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            294 => Self::AlterMigrationCommand,
            295 => Self::AlterMigrationCommandsList_Semicolons_AlterMigrationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            296 => Self::ALTER_MIGRATION_NodeName_AlterMigrationCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterModuleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            297 => Self::ALTER_MODULE_ModuleName_AlterCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            298 => Self::AlterAccessPolicyStmt,
            299 => Self::AlterAnnotationValueStmt,
            300 => Self::AlterConcreteConstraintStmt,
            301 => Self::AlterConcreteIndexStmt,
            302 => Self::AlterConcreteLinkStmt,
            303 => Self::AlterConcretePropertyStmt,
            304 => Self::AlterSimpleExtending,
            305 => Self::AlterTriggerStmt,
            306 => Self::CreateAccessPolicyStmt,
            307 => Self::CreateAnnotationValueStmt,
            308 => Self::CreateConcreteConstraintStmt,
            309 => Self::CreateConcreteIndexStmt,
            310 => Self::CreateConcreteLinkStmt,
            311 => Self::CreateConcretePropertyStmt,
            312 => Self::CreateTriggerStmt,
            313 => Self::DropAccessPolicyStmt,
            314 => Self::DropAnnotationValueStmt,
            315 => Self::DropConcreteConstraintStmt,
            316 => Self::DropConcreteIndexStmt,
            317 => Self::DropConcreteLinkStmt,
            318 => Self::DropConcretePropertyStmt,
            319 => Self::DropTriggerStmt,
            320 => Self::RenameStmt,
            321 => Self::ResetFieldStmt,
            322 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            323 => Self::AlterObjectTypeCommand,
            324 => Self::LBRACE_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
            325 => Self::LBRACE_OptSemicolons_RBRACE,
            326 => Self::LBRACE_Semicolons_AlterObjectTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            327 => Self::AlterObjectTypeCommand,
            328 => Self::AlterObjectTypeCommandsList_Semicolons_AlterObjectTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterObjectTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            329 => Self::ALTER_TYPE_NodeName_AlterObjectTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorCommand {
    fn from_id(id: usize) -> Self {
        match id {
            330 => Self::AlterAnnotationValueStmt,
            331 => Self::CreateAnnotationValueStmt,
            332 => Self::DropAnnotationValueStmt,
            333 => Self::ResetFieldStmt,
            334 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            335 => Self::AlterOperatorCommand,
            336 => Self::LBRACE_AlterOperatorCommandsList_OptSemicolons_RBRACE,
            337 => Self::LBRACE_OptSemicolons_RBRACE,
            338 => Self::LBRACE_Semicolons_AlterOperatorCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            339 => Self::AlterOperatorCommand,
            340 => Self::AlterOperatorCommandsList_Semicolons_AlterOperatorCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOperatorStmt {
    fn from_id(id: usize) -> Self {
        match id {
            341 => Self::ALTER_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_AlterOperatorCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterOwnedStmt {
    fn from_id(id: usize) -> Self {
        match id {
            342 => Self::DROP_OWNED,
            343 => Self::SET_OWNED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            344 => Self::AlterAnnotationValueStmt,
            345 => Self::AlterRewriteStmt,
            346 => Self::CreateAnnotationValueStmt,
            347 => Self::CreateRewriteStmt,
            348 => Self::DropAnnotationValueStmt,
            349 => Self::DropRewriteStmt,
            350 => Self::RenameStmt,
            351 => Self::ResetFieldStmt,
            352 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            353 => Self::AlterPropertyCommand,
            354 => Self::LBRACE_AlterPropertyCommandsList_OptSemicolons_RBRACE,
            355 => Self::LBRACE_OptSemicolons_RBRACE,
            356 => Self::LBRACE_Semicolons_AlterPropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            357 => Self::AlterPropertyCommand,
            358 => Self::AlterPropertyCommandsList_Semicolons_AlterPropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterPropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            359 => Self::ALTER_ABSTRACT_PROPERTY_PtrNodeName_AlterPropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteCommand {
    fn from_id(id: usize) -> Self {
        match id {
            360 => Self::AlterAnnotationValueStmt,
            361 => Self::CreateAnnotationValueStmt,
            362 => Self::DropAnnotationValueStmt,
            363 => Self::ResetFieldStmt,
            364 => Self::SetFieldStmt,
            365 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            366 => Self::AlterRewriteCommand,
            367 => Self::LBRACE_AlterRewriteCommandsList_OptSemicolons_RBRACE,
            368 => Self::LBRACE_OptSemicolons_RBRACE,
            369 => Self::LBRACE_Semicolons_AlterRewriteCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            370 => Self::AlterRewriteCommand,
            371 => Self::AlterRewriteCommandsList_Semicolons_AlterRewriteCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRewriteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            372 => Self::ALTER_REWRITE_RewriteKindList_AlterRewriteCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleCommand {
    fn from_id(id: usize) -> Self {
        match id {
            373 => Self::AlterRoleExtending,
            374 => Self::RenameStmt,
            375 => Self::ResetFieldStmt,
            376 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            377 => Self::AlterRoleCommand,
            378 => Self::LBRACE_AlterRoleCommandsList_OptSemicolons_RBRACE,
            379 => Self::LBRACE_OptSemicolons_RBRACE,
            380 => Self::LBRACE_Semicolons_AlterRoleCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            381 => Self::AlterRoleCommand,
            382 => Self::AlterRoleCommandsList_Semicolons_AlterRoleCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleExtending {
    fn from_id(id: usize) -> Self {
        match id {
            383 => Self::DROP_EXTENDING_ShortNodeNameList,
            384 => Self::EXTENDING_ShortNodeNameList_OptPosition,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterRoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            385 => Self::ALTER_ROLE_ShortNodeName_AlterRoleCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            386 => Self::AlterAnnotationValueStmt,
            387 => Self::AlterConcreteConstraintStmt,
            388 => Self::AlterExtending,
            389 => Self::CreateAnnotationValueStmt,
            390 => Self::CreateConcreteConstraintStmt,
            391 => Self::DropAnnotationValueStmt,
            392 => Self::DropConcreteConstraintStmt,
            393 => Self::RenameStmt,
            394 => Self::ResetFieldStmt,
            395 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            396 => Self::AlterScalarTypeCommand,
            397 => Self::LBRACE_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
            398 => Self::LBRACE_OptSemicolons_RBRACE,
            399 => Self::LBRACE_Semicolons_AlterScalarTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            400 => Self::AlterScalarTypeCommand,
            401 => Self::AlterScalarTypeCommandsList_Semicolons_AlterScalarTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterScalarTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            402 => Self::ALTER_SCALAR_TYPE_NodeName_AlterScalarTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterSimpleExtending {
    fn from_id(id: usize) -> Self {
        match id {
            403 => Self::AlterAbstract,
            404 => Self::DROP_EXTENDING_SimpleTypeNameList,
            405 => Self::EXTENDING_SimpleTypeNameList_OptPosition,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerCommand {
    fn from_id(id: usize) -> Self {
        match id {
            406 => Self::AccessWhenStmt,
            407 => Self::AlterAnnotationValueStmt,
            408 => Self::CreateAnnotationValueStmt,
            409 => Self::DropAnnotationValueStmt,
            410 => Self::RenameStmt,
            411 => Self::ResetFieldStmt,
            412 => Self::SetFieldStmt,
            413 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            414 => Self::AlterTriggerCommand,
            415 => Self::LBRACE_AlterTriggerCommandsList_OptSemicolons_RBRACE,
            416 => Self::LBRACE_OptSemicolons_RBRACE,
            417 => Self::LBRACE_Semicolons_AlterTriggerCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            418 => Self::AlterTriggerCommand,
            419 => Self::AlterTriggerCommandsList_Semicolons_AlterTriggerCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AlterTriggerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            420 => Self::ALTER_TRIGGER_UnqualifiedPointerName_AlterTriggerCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnalyzeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            421 => Self::ANALYZE_ExprStmt,
            422 => Self::ANALYZE_NamedTuple_ExprStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnnotationDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            423 => Self::ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
            424 => Self::ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple_CreateSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnnotationDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            425 => Self::ABSTRACT_ANNOTATION_NodeName_OptExtendingSimple,
            426 => Self::ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnyIdentifier {
    fn from_id(id: usize) -> Self {
        match id {
            427 => Self::PtrIdentifier,
            428 => Self::ReservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AnyNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            429 => Self::AnyIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AtomicExpr {
    fn from_id(id: usize) -> Self {
        match id {
            430 => Self::AtomicPath,
            431 => Self::BaseAtomicExpr,
            432 => Self::LANGBRACKET_FullTypeExpr_RANGBRACKET_AtomicExpr_P_TYPECAST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::AtomicPath {
    fn from_id(id: usize) -> Self {
        match id {
            433 => Self::AtomicExpr_PathStep_P_DOT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseAtomicExpr {
    fn from_id(id: usize) -> Self {
        match id {
            434 => Self::Collection,
            435 => Self::Constant,
            436 => Self::DUNDERDEFAULT,
            437 => Self::DUNDERNEW,
            438 => Self::DUNDEROLD,
            439 => Self::DUNDERSOURCE,
            440 => Self::DUNDERSPECIFIED,
            441 => Self::DUNDERSUBJECT,
            442 => Self::FreeShape,
            443 => Self::FuncExpr,
            444 => Self::NamedTuple,
            445 => Self::NodeName_P_DOT,
            446 => Self::ParenExpr_P_UMINUS,
            447 => Self::PathStep_P_DOT,
            448 => Self::Set,
            449 => Self::Tuple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseBooleanConstant {
    fn from_id(id: usize) -> Self {
        match id {
            450 => Self::FALSE,
            451 => Self::TRUE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseBytesConstant {
    fn from_id(id: usize) -> Self {
        match id {
            452 => Self::BCONST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseName {
    fn from_id(id: usize) -> Self {
        match id {
            453 => Self::Identifier,
            454 => Self::QualifiedName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseNumberConstant {
    fn from_id(id: usize) -> Self {
        match id {
            455 => Self::FCONST,
            456 => Self::ICONST,
            457 => Self::NFCONST,
            458 => Self::NICONST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BaseStringConstant {
    fn from_id(id: usize) -> Self {
        match id {
            459 => Self::SCONST,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BranchOptions {
    fn from_id(id: usize) -> Self {
        match id {
            460 => Self::FORCE,
            461 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::BranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            462 => Self::AlterBranchStmt,
            463 => Self::CreateBranchStmt,
            464 => Self::DropBranchStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ByClause {
    fn from_id(id: usize) -> Self {
        match id {
            465 => Self::BY_GroupingElementList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CastAllowedUse {
    fn from_id(id: usize) -> Self {
        match id {
            466 => Self::ALLOW_ASSIGNMENT,
            467 => Self::ALLOW_IMPLICIT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CastCode {
    fn from_id(id: usize) -> Self {
        match id {
            468 => Self::USING_Identifier_BaseStringConstant,
            469 => Self::USING_Identifier_CAST,
            470 => Self::USING_Identifier_EXPRESSION,
            471 => Self::USING_Identifier_FUNCTION_BaseStringConstant,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Collection {
    fn from_id(id: usize) -> Self {
        match id {
            472 => Self::LBRACKET_OptExprList_RBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CollectionTypeName {
    fn from_id(id: usize) -> Self {
        match id {
            473 => Self::NodeName_LANGBRACKET_RANGBRACKET,
            474 => Self::NodeName_LANGBRACKET_SubtypeList_RANGBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ColonedIdents {
    fn from_id(id: usize) -> Self {
        match id {
            475 => Self::AnyIdentifier,
            476 => Self::ColonedIdents_DOUBLECOLON_AnyIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CommitMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            477 => Self::COMMIT_MIGRATION,
            478 => Self::COMMIT_MIGRATION_REWRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CompareOp {
    fn from_id(id: usize) -> Self {
        match id {
            479 => Self::DISTINCTFROM_P_COMPARE_OP,
            480 => Self::EQUALS_P_COMPARE_OP,
            481 => Self::GREATEREQ_P_COMPARE_OP,
            482 => Self::LANGBRACKET_P_COMPARE_OP,
            483 => Self::LESSEQ_P_COMPARE_OP,
            484 => Self::NOTDISTINCTFROM_P_COMPARE_OP,
            485 => Self::NOTEQ_P_COMPARE_OP,
            486 => Self::RANGBRACKET_P_COMPARE_OP,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ComputableShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            487 => Self::MULTI_SimpleShapePointer_ASSIGN_Expr,
            488 => Self::OPTIONAL_MULTI_SimpleShapePointer_ASSIGN_Expr,
            489 => Self::OPTIONAL_SINGLE_SimpleShapePointer_ASSIGN_Expr,
            490 => Self::OPTIONAL_SimpleShapePointer_ASSIGN_Expr,
            491 => Self::REQUIRED_MULTI_SimpleShapePointer_ASSIGN_Expr,
            492 => Self::REQUIRED_SINGLE_SimpleShapePointer_ASSIGN_Expr,
            493 => Self::REQUIRED_SimpleShapePointer_ASSIGN_Expr,
            494 => Self::SINGLE_SimpleShapePointer_ASSIGN_Expr,
            495 => Self::SimpleShapePointer_ADDASSIGN_Expr,
            496 => Self::SimpleShapePointer_ASSIGN_Expr,
            497 => Self::SimpleShapePointer_REMASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteConstraintBlock {
    fn from_id(id: usize) -> Self {
        match id {
            498 => Self::CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
            499 => Self::DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_CreateSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteConstraintShort {
    fn from_id(id: usize) -> Self {
        match id {
            500 => Self::CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
            501 => Self::DELEGATED_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteIndexDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            502 => Self::DEFERRED_INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            503 => Self::INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            504 => Self::DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            505 => Self::DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            506 => Self::INDEX_NodeName_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
            507 => Self::INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteIndexDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            508 => Self::DEFERRED_INDEX_NodeName_OnExpr_OptExceptExpr,
            509 => Self::DEFERRED_INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
            510 => Self::INDEX_NodeName_OnExpr_OptExceptExpr,
            511 => Self::INDEX_NodeName_IndexExtArgList_OnExpr_OptExceptExpr,
            512 => Self::DEFERRED_INDEX_OnExpr_OptExceptExpr,
            513 => Self::INDEX_OnExpr_OptExceptExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteLinkBlock {
    fn from_id(id: usize) -> Self {
        match id {
            514 => Self::OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            515 => Self::OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            516 => Self::LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            517 => Self::PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteLinkShort {
    fn from_id(id: usize) -> Self {
        match id {
            518 => Self::LINK_PathNodeName_ASSIGN_Expr,
            519 => Self::OVERLOADED_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
            520 => Self::OVERLOADED_PtrQuals_LINK_PathNodeName_OptExtendingSimple_OptPtrTarget,
            521 => Self::PtrQuals_LINK_PathNodeName_ASSIGN_Expr,
            522 => Self::LINK_PathNodeName_OptExtendingSimple_PtrTarget,
            523 => Self::PtrQuals_LINK_PathNodeName_OptExtendingSimple_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcretePropertyBlock {
    fn from_id(id: usize) -> Self {
        match id {
            524 => Self::OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
            525 => Self::OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
            526 => Self::PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
            527 => Self::PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcretePropertySDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcretePropertyShort {
    fn from_id(id: usize) -> Self {
        match id {
            528 => Self::PROPERTY_PathNodeName_ASSIGN_Expr,
            529 => Self::OVERLOADED_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
            530 => Self::OVERLOADED_PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_OptPtrTarget,
            531 => Self::PtrQuals_PROPERTY_PathNodeName_ASSIGN_Expr,
            532 => Self::PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
            533 => Self::PtrQuals_PROPERTY_PathNodeName_OptExtendingSimple_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteUnknownPointerBlock {
    fn from_id(id: usize) -> Self {
        match id {
            534 => Self::OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            535 => Self::OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            536 => Self::PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
            537 => Self::PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget_CreateConcreteLinkSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteUnknownPointerObjectShort {
    fn from_id(id: usize) -> Self {
        match id {
            538 => Self::PathNodeName_ASSIGN_Expr,
            539 => Self::PtrQuals_PathNodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConcreteUnknownPointerShort {
    fn from_id(id: usize) -> Self {
        match id {
            540 => Self::OVERLOADED_PathNodeName_OptExtendingSimple_OptPtrTarget,
            541 => Self::OVERLOADED_PtrQuals_PathNodeName_OptExtendingSimple_OptPtrTarget,
            542 => Self::PathNodeName_OptExtendingSimple_PtrTarget,
            543 => Self::PtrQuals_PathNodeName_OptExtendingSimple_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConfigOp {
    fn from_id(id: usize) -> Self {
        match id {
            544 => Self::INSERT_NodeName_Shape,
            545 => Self::RESET_NodeName_OptFilterClause,
            546 => Self::SET_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConfigScope {
    fn from_id(id: usize) -> Self {
        match id {
            547 => Self::CURRENT_BRANCH,
            548 => Self::CURRENT_DATABASE,
            549 => Self::INSTANCE,
            550 => Self::SESSION,
            551 => Self::SYSTEM,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConfigStmt {
    fn from_id(id: usize) -> Self {
        match id {
            552 => Self::CONFIGURE_BRANCH_ConfigOp,
            553 => Self::CONFIGURE_ConfigScope_ConfigOp,
            554 => Self::CONFIGURE_DATABASE_ConfigOp,
            555 => Self::RESET_GLOBAL_NodeName,
            556 => Self::SET_GLOBAL_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Constant {
    fn from_id(id: usize) -> Self {
        match id {
            557 => Self::BaseBooleanConstant,
            558 => Self::BaseBytesConstant,
            559 => Self::BaseNumberConstant,
            560 => Self::BaseStringConstant,
            561 => Self::PARAMETER,
            562 => Self::PARAMETERANDTYPE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConstraintDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            563 => Self::ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
            564 => Self::ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_CreateSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ConstraintDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            565 => Self::ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple,
            566 => Self::ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            567 => Self::CreateAnnotationValueStmt,
            568 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            569 => Self::LBRACE_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
            570 => Self::LBRACE_OptSemicolons_RBRACE,
            571 => Self::LBRACE_Semicolons_CreateAccessPolicyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            572 => Self::CreateAccessPolicyCommand,
            573 => Self::CreateAccessPolicyCommandsList_Semicolons_CreateAccessPolicyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            575 => Self::CreateAccessPolicySDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            576 => Self::SetAnnotation,
            577 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            578 => Self::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
            579 => Self::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandShort_RBRACE,
            580 => Self::LBRACE_OptSemicolons_CreateAccessPolicySDLCommandsList_OptSemicolons_RBRACE,
            581 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicySDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            582 => Self::CreateAccessPolicySDLCommandFull,
            583 => Self::CreateAccessPolicySDLCommandsList_OptSemicolons_CreateAccessPolicySDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAccessPolicyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            584 => Self::CREATE_ACCESS_POLICY_UnqualifiedPointerName_OptWhenBlock_AccessPolicyAction_AccessKindList_OptUsingBlock_OptCreateAccessPolicyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasCommand {
    fn from_id(id: usize) -> Self {
        match id {
            585 => Self::AlterAnnotationValueStmt,
            586 => Self::CreateAnnotationValueStmt,
            587 => Self::SetFieldStmt,
            588 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            589 => Self::CreateAliasCommand,
            590 => Self::LBRACE_CreateAliasCommandsList_OptSemicolons_RBRACE,
            591 => Self::LBRACE_OptSemicolons_RBRACE,
            592 => Self::LBRACE_Semicolons_CreateAliasCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            593 => Self::CreateAliasCommand,
            594 => Self::CreateAliasCommandsList_Semicolons_CreateAliasCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            596 => Self::CreateAliasSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            597 => Self::SetAnnotation,
            598 => Self::SetField,
            599 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            600 => Self::LBRACE_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
            601 => Self::LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandShort_RBRACE,
            602 => Self::LBRACE_OptSemicolons_CreateAliasSDLCommandsList_OptSemicolons_RBRACE,
            603 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            604 => Self::CreateAliasSDLCommandFull,
            605 => Self::CreateAliasSDLCommandsList_OptSemicolons_CreateAliasSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasSingleSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            607 => Self::CreateAliasSDLCommandShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAliasStmt {
    fn from_id(id: usize) -> Self {
        match id {
            608 => Self::CREATE_ALIAS_NodeName_CreateAliasCommandsBlock,
            609 => Self::CREATE_ALIAS_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            610 => Self::CreateAnnotationValueStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            611 => Self::LBRACE_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
            612 => Self::LBRACE_OptSemicolons_RBRACE,
            613 => Self::LBRACE_Semicolons_CreateAnnotationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            614 => Self::CreateAnnotationCommand,
            615 => Self::CreateAnnotationCommandsList_Semicolons_CreateAnnotationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            616 => Self::CREATE_ABSTRACT_ANNOTATION_NodeName_OptCreateAnnotationCommandsBlock,
            617 => Self::CREATE_ABSTRACT_INHERITABLE_ANNOTATION_NodeName_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateAnnotationValueStmt {
    fn from_id(id: usize) -> Self {
        match id {
            618 => Self::CREATE_ANNOTATION_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateBranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            619 => Self::CREATE_EMPTY_BRANCH_DatabaseName,
            620 => Self::CREATE_DATA_BRANCH_DatabaseName_FROM_DatabaseName,
            621 => Self::CREATE_SCHEMA_BRANCH_DatabaseName_FROM_DatabaseName,
            622 => Self::CREATE_TEMPLATE_BRANCH_DatabaseName_FROM_DatabaseName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastCommand {
    fn from_id(id: usize) -> Self {
        match id {
            623 => Self::AlterAnnotationValueStmt,
            624 => Self::CastAllowedUse,
            625 => Self::CastCode,
            626 => Self::CreateAnnotationValueStmt,
            627 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            628 => Self::CreateCastCommand,
            629 => Self::LBRACE_CreateCastCommandsList_OptSemicolons_RBRACE,
            630 => Self::LBRACE_OptSemicolons_RBRACE,
            631 => Self::LBRACE_Semicolons_CreateCastCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            632 => Self::CreateCastCommand,
            633 => Self::CreateCastCommandsList_Semicolons_CreateCastCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCastStmt {
    fn from_id(id: usize) -> Self {
        match id {
            634 => Self::CREATE_CAST_FROM_TypeName_TO_TypeName_CreateCastCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCommand {
    fn from_id(id: usize) -> Self {
        match id {
            635 => Self::AlterAnnotationValueStmt,
            636 => Self::CreateAnnotationValueStmt,
            637 => Self::SetFieldStmt,
            638 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            639 => Self::LBRACE_CreateCommandsList_OptSemicolons_RBRACE,
            640 => Self::LBRACE_OptSemicolons_RBRACE,
            641 => Self::LBRACE_Semicolons_CreateCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            642 => Self::CreateCommand,
            643 => Self::CreateCommandsList_Semicolons_CreateCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            644 => Self::CREATE_OptDelegated_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            646 => Self::CreateConcreteIndexSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            647 => Self::SetAnnotation,
            648 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            649 => Self::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
            650 => Self::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandShort_RBRACE,
            651 => Self::LBRACE_OptSemicolons_CreateConcreteIndexSDLCommandsList_OptSemicolons_RBRACE,
            652 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            653 => Self::CreateConcreteIndexSDLCommandFull,
            654 => Self::CreateConcreteIndexSDLCommandsList_OptSemicolons_CreateConcreteIndexSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            655 => Self::CREATE_OptDeferred_INDEX_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
            656 => Self::CREATE_OptDeferred_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            657 => Self::AlterAnnotationValueStmt,
            658 => Self::CreateAnnotationValueStmt,
            659 => Self::CreateConcreteConstraintStmt,
            660 => Self::CreateConcreteIndexStmt,
            661 => Self::CreateConcretePropertyStmt,
            662 => Self::CreateRewriteStmt,
            663 => Self::CreateSimpleExtending,
            664 => Self::OnSourceDeleteStmt,
            665 => Self::OnTargetDeleteStmt,
            666 => Self::SetFieldStmt,
            667 => Self::SetRequiredInCreateStmt,
            668 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            669 => Self::LBRACE_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
            670 => Self::LBRACE_OptSemicolons_RBRACE,
            671 => Self::LBRACE_Semicolons_CreateConcreteLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            672 => Self::CreateConcreteLinkCommand,
            673 => Self::CreateConcreteLinkCommandsList_Semicolons_CreateConcreteLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            674 => Self::ConcreteConstraintBlock,
            675 => Self::ConcreteIndexDeclarationBlock,
            676 => Self::ConcretePropertyBlock,
            677 => Self::ConcreteUnknownPointerBlock,
            678 => Self::RewriteDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            679 => Self::CreateConcreteLinkSDLCommandBlock,
            680 => Self::CreateConcreteLinkSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            681 => Self::ConcreteConstraintShort,
            682 => Self::ConcreteIndexDeclarationShort,
            683 => Self::ConcretePropertyShort,
            684 => Self::ConcreteUnknownPointerShort,
            685 => Self::CreateSimpleExtending,
            686 => Self::OnSourceDeleteStmt,
            687 => Self::OnTargetDeleteStmt,
            688 => Self::RewriteDeclarationShort,
            689 => Self::SetAnnotation,
            690 => Self::SetField,
            691 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            692 => Self::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
            693 => Self::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandShort_RBRACE,
            694 => Self::LBRACE_OptSemicolons_CreateConcreteLinkSDLCommandsList_OptSemicolons_RBRACE,
            695 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            696 => Self::CreateConcreteLinkSDLCommandFull,
            697 => Self::CreateConcreteLinkSDLCommandsList_OptSemicolons_CreateConcreteLinkSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcreteLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            698 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_ASSIGN_Expr,
            699 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptCreateConcreteLinkCommandsBlock,
            700 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
            701 => Self::CREATE_OptPtrQuals_LINK_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcreteLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            702 => Self::AlterAnnotationValueStmt,
            703 => Self::CreateAnnotationValueStmt,
            704 => Self::CreateConcreteConstraintStmt,
            705 => Self::CreateRewriteStmt,
            706 => Self::CreateSimpleExtending,
            707 => Self::SetFieldStmt,
            708 => Self::SetRequiredInCreateStmt,
            709 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            710 => Self::LBRACE_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
            711 => Self::LBRACE_OptSemicolons_RBRACE,
            712 => Self::LBRACE_Semicolons_CreateConcretePropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            713 => Self::CreateConcretePropertyCommand,
            714 => Self::CreateConcretePropertyCommandsList_Semicolons_CreateConcretePropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            715 => Self::ConcreteConstraintBlock,
            716 => Self::RewriteDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            717 => Self::CreateConcretePropertySDLCommandBlock,
            718 => Self::CreateConcretePropertySDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            719 => Self::ConcreteConstraintShort,
            720 => Self::CreateSimpleExtending,
            721 => Self::RewriteDeclarationShort,
            722 => Self::SetAnnotation,
            723 => Self::SetField,
            724 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            725 => Self::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
            726 => Self::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandShort_RBRACE,
            727 => Self::LBRACE_OptSemicolons_CreateConcretePropertySDLCommandsList_OptSemicolons_RBRACE,
            728 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertySDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            729 => Self::CreateConcretePropertySDLCommandFull,
            730 => Self::CreateConcretePropertySDLCommandsList_OptSemicolons_CreateConcretePropertySDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConcretePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            731 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_ASSIGN_Expr,
            732 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptCreateConcretePropertyCommandsBlock,
            733 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_ARROW_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
            734 => Self::CREATE_OptPtrQuals_PROPERTY_UnqualifiedPointerName_OptExtendingSimple_COLON_FullTypeExpr_OptCreateConcretePropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            735 => Self::CREATE_ABSTRACT_CONSTRAINT_NodeName_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
            736 => Self::CREATE_ABSTRACT_CONSTRAINT_NodeName_CreateFunctionArgs_OptOnExpr_OptExtendingSimple_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseCommand {
    fn from_id(id: usize) -> Self {
        match id {
            737 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            738 => Self::LBRACE_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
            739 => Self::LBRACE_OptSemicolons_RBRACE,
            740 => Self::LBRACE_Semicolons_CreateDatabaseCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            741 => Self::CreateDatabaseCommand,
            742 => Self::CreateDatabaseCommandsList_Semicolons_CreateDatabaseCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateDatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            743 => Self::CREATE_DATABASE_DatabaseName_FROM_AnyNodeName_OptCreateDatabaseCommandsBlock,
            744 => Self::CREATE_DATABASE_DatabaseName_OptCreateDatabaseCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionCommand {
    fn from_id(id: usize) -> Self {
        match id {
            745 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            746 => Self::LBRACE_CreateExtensionCommandsList_OptSemicolons_RBRACE,
            747 => Self::LBRACE_OptSemicolons_RBRACE,
            748 => Self::LBRACE_Semicolons_CreateExtensionCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            749 => Self::CreateExtensionCommand,
            750 => Self::CreateExtensionCommandsList_Semicolons_CreateExtensionCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageCommand {
    fn from_id(id: usize) -> Self {
        match id {
            751 => Self::NestedQLBlockStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            752 => Self::LBRACE_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
            753 => Self::LBRACE_OptSemicolons_RBRACE,
            754 => Self::LBRACE_Semicolons_CreateExtensionPackageCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            755 => Self::CreateExtensionPackageCommand,
            756 => Self::CreateExtensionPackageCommandsList_Semicolons_CreateExtensionPackageCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionPackageStmt {
    fn from_id(id: usize) -> Self {
        match id {
            757 => Self::CREATE_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion_OptCreateExtensionPackageCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            758 => Self::CREATE_EXTENSION_ShortNodeName_OptExtensionVersion_OptCreateExtensionCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionArgs {
    fn from_id(id: usize) -> Self {
        match id {
            759 => Self::LPAREN_FuncDeclArgs_RPAREN,
            760 => Self::LPAREN_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionCommand {
    fn from_id(id: usize) -> Self {
        match id {
            761 => Self::AlterAnnotationValueStmt,
            762 => Self::CreateAnnotationValueStmt,
            763 => Self::FromFunction,
            764 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            765 => Self::CreateFunctionCommand,
            766 => Self::LBRACE_CreateFunctionCommandsList_OptSemicolons_RBRACE,
            767 => Self::LBRACE_OptSemicolons_RBRACE,
            768 => Self::LBRACE_Semicolons_CreateFunctionCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            769 => Self::CreateFunctionCommand,
            770 => Self::CreateFunctionCommandsList_Semicolons_CreateFunctionCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            772 => Self::CreateFunctionSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            773 => Self::FromFunction,
            774 => Self::SetAnnotation,
            775 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            776 => Self::LBRACE_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
            777 => Self::LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandShort_RBRACE,
            778 => Self::LBRACE_OptSemicolons_CreateFunctionSDLCommandsList_OptSemicolons_RBRACE,
            779 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            780 => Self::CreateFunctionSDLCommandFull,
            781 => Self::CreateFunctionSDLCommandsList_OptSemicolons_CreateFunctionSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionSingleSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            783 => Self::CreateFunctionSDLCommandShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFunctionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            784 => Self::CREATE_FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateFutureStmt {
    fn from_id(id: usize) -> Self {
        match id {
            785 => Self::CREATE_FUTURE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalCommand {
    fn from_id(id: usize) -> Self {
        match id {
            786 => Self::CreateAnnotationValueStmt,
            787 => Self::SetFieldStmt,
            788 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            789 => Self::LBRACE_CreateGlobalCommandsList_OptSemicolons_RBRACE,
            790 => Self::LBRACE_OptSemicolons_RBRACE,
            791 => Self::LBRACE_Semicolons_CreateGlobalCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            792 => Self::CreateGlobalCommand,
            793 => Self::CreateGlobalCommandsList_Semicolons_CreateGlobalCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            795 => Self::CreateGlobalSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            796 => Self::SetAnnotation,
            797 => Self::SetField,
            798 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            799 => Self::LBRACE_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
            800 => Self::LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandShort_RBRACE,
            801 => Self::LBRACE_OptSemicolons_CreateGlobalSDLCommandsList_OptSemicolons_RBRACE,
            802 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            803 => Self::CreateGlobalSDLCommandFull,
            804 => Self::CreateGlobalSDLCommandsList_OptSemicolons_CreateGlobalSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateGlobalStmt {
    fn from_id(id: usize) -> Self {
        match id {
            805 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
            806 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_OptCreateConcretePropertyCommandsBlock,
            807 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_ARROW_FullTypeExpr_OptCreateGlobalCommandsBlock,
            808 => Self::CREATE_OptPtrQuals_GLOBAL_NodeName_COLON_FullTypeExpr_OptCreateGlobalCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            809 => Self::AlterAnnotationValueStmt,
            810 => Self::CreateAnnotationValueStmt,
            811 => Self::SetFieldStmt,
            812 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            813 => Self::LBRACE_CreateIndexCommandsList_OptSemicolons_RBRACE,
            814 => Self::LBRACE_OptSemicolons_RBRACE,
            815 => Self::LBRACE_Semicolons_CreateIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            816 => Self::CreateIndexCommand,
            817 => Self::CreateIndexCommandsList_Semicolons_CreateIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchCommand {
    fn from_id(id: usize) -> Self {
        match id {
            818 => Self::CreateAnnotationValueStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            819 => Self::LBRACE_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
            820 => Self::LBRACE_OptSemicolons_RBRACE,
            821 => Self::LBRACE_Semicolons_CreateIndexMatchCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            822 => Self::CreateIndexMatchCommand,
            823 => Self::CreateIndexMatchCommandsList_Semicolons_CreateIndexMatchCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexMatchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            824 => Self::CREATE_INDEX_MATCH_FOR_TypeName_USING_NodeName_OptCreateIndexMatchCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            826 => Self::CreateIndexSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            827 => Self::SetAnnotation,
            828 => Self::SetField,
            829 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            830 => Self::LBRACE_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
            831 => Self::LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandShort_RBRACE,
            832 => Self::LBRACE_OptSemicolons_CreateIndexSDLCommandsList_OptSemicolons_RBRACE,
            833 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            834 => Self::CreateIndexSDLCommandFull,
            835 => Self::CreateIndexSDLCommandsList_OptSemicolons_CreateIndexSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            836 => Self::CREATE_ABSTRACT_INDEX_NodeName_OptExtendingSimple_OptCreateIndexCommandsBlock,
            837 => Self::CREATE_ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_OptCreateIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            838 => Self::AlterAnnotationValueStmt,
            839 => Self::CreateAnnotationValueStmt,
            840 => Self::CreateConcreteConstraintStmt,
            841 => Self::CreateConcreteIndexStmt,
            842 => Self::CreateConcretePropertyStmt,
            843 => Self::CreateRewriteStmt,
            844 => Self::CreateSimpleExtending,
            845 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            846 => Self::LBRACE_CreateLinkCommandsList_OptSemicolons_RBRACE,
            847 => Self::LBRACE_OptSemicolons_RBRACE,
            848 => Self::LBRACE_Semicolons_CreateLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            849 => Self::CreateLinkCommand,
            850 => Self::CreateLinkCommandsList_Semicolons_CreateLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            851 => Self::ConcreteConstraintBlock,
            852 => Self::ConcreteIndexDeclarationBlock,
            853 => Self::ConcretePropertyBlock,
            854 => Self::ConcreteUnknownPointerBlock,
            855 => Self::RewriteDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            856 => Self::CreateLinkSDLCommandBlock,
            857 => Self::CreateLinkSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            858 => Self::ConcreteConstraintShort,
            859 => Self::ConcreteIndexDeclarationShort,
            860 => Self::ConcretePropertyShort,
            861 => Self::ConcreteUnknownPointerShort,
            862 => Self::CreateSimpleExtending,
            863 => Self::RewriteDeclarationShort,
            864 => Self::SetAnnotation,
            865 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            866 => Self::LBRACE_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
            867 => Self::LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandShort_RBRACE,
            868 => Self::LBRACE_OptSemicolons_CreateLinkSDLCommandsList_OptSemicolons_RBRACE,
            869 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            870 => Self::CreateLinkSDLCommandFull,
            871 => Self::CreateLinkSDLCommandsList_OptSemicolons_CreateLinkSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            872 => Self::CREATE_ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_OptCreateLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationCommand {
    fn from_id(id: usize) -> Self {
        match id {
            873 => Self::NestedQLBlockStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            874 => Self::LBRACE_CreateMigrationCommandsList_OptSemicolons_RBRACE,
            875 => Self::LBRACE_OptSemicolons_RBRACE,
            876 => Self::LBRACE_Semicolons_CreateMigrationCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            877 => Self::CreateMigrationCommand,
            878 => Self::CreateMigrationCommandsList_Semicolons_CreateMigrationCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            879 => Self::CREATE_APPLIED_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
            880 => Self::CREATE_MIGRATION_OptMigrationNameParentName_OptCreateMigrationCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateModuleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            881 => Self::CREATE_MODULE_ModuleName_OptIfNotExists_OptCreateCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            882 => Self::AlterAccessPolicyStmt,
            883 => Self::AlterAnnotationValueStmt,
            884 => Self::AlterConcreteConstraintStmt,
            885 => Self::AlterConcreteIndexStmt,
            886 => Self::AlterConcreteLinkStmt,
            887 => Self::AlterConcretePropertyStmt,
            888 => Self::AlterTriggerStmt,
            889 => Self::CreateAccessPolicyStmt,
            890 => Self::CreateAnnotationValueStmt,
            891 => Self::CreateConcreteConstraintStmt,
            892 => Self::CreateConcreteIndexStmt,
            893 => Self::CreateConcreteLinkStmt,
            894 => Self::CreateConcretePropertyStmt,
            895 => Self::CreateTriggerStmt,
            896 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            897 => Self::LBRACE_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
            898 => Self::LBRACE_OptSemicolons_RBRACE,
            899 => Self::LBRACE_Semicolons_CreateObjectTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            900 => Self::CreateObjectTypeCommand,
            901 => Self::CreateObjectTypeCommandsList_Semicolons_CreateObjectTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            902 => Self::AccessPolicyDeclarationBlock,
            903 => Self::ConcreteConstraintBlock,
            904 => Self::ConcreteIndexDeclarationBlock,
            905 => Self::ConcreteLinkBlock,
            906 => Self::ConcretePropertyBlock,
            907 => Self::ConcreteUnknownPointerBlock,
            908 => Self::TriggerDeclarationBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            909 => Self::CreateObjectTypeSDLCommandBlock,
            910 => Self::CreateObjectTypeSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            911 => Self::AccessPolicyDeclarationShort,
            912 => Self::ConcreteConstraintShort,
            913 => Self::ConcreteIndexDeclarationShort,
            914 => Self::ConcreteLinkShort,
            915 => Self::ConcretePropertyShort,
            916 => Self::ConcreteUnknownPointerObjectShort,
            917 => Self::ConcreteUnknownPointerShort,
            918 => Self::SetAnnotation,
            919 => Self::TriggerDeclarationShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            920 => Self::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
            921 => Self::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandShort_RBRACE,
            922 => Self::LBRACE_OptSemicolons_CreateObjectTypeSDLCommandsList_OptSemicolons_RBRACE,
            923 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            924 => Self::CreateObjectTypeSDLCommandFull,
            925 => Self::CreateObjectTypeSDLCommandsList_OptSemicolons_CreateObjectTypeSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateObjectTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            926 => Self::CREATE_ABSTRACT_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
            927 => Self::CREATE_TYPE_NodeName_OptExtendingSimple_OptCreateObjectTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorCommand {
    fn from_id(id: usize) -> Self {
        match id {
            928 => Self::AlterAnnotationValueStmt,
            929 => Self::CreateAnnotationValueStmt,
            930 => Self::OperatorCode,
            931 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            932 => Self::CreateOperatorCommand,
            933 => Self::LBRACE_CreateOperatorCommandsList_OptSemicolons_RBRACE,
            934 => Self::LBRACE_OptSemicolons_RBRACE,
            935 => Self::LBRACE_Semicolons_CreateOperatorCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            936 => Self::CreateOperatorCommand,
            937 => Self::CreateOperatorCommandsList_Semicolons_CreateOperatorCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateOperatorStmt {
    fn from_id(id: usize) -> Self {
        match id {
            938 => Self::CREATE_ABSTRACT_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_OptCreateOperatorCommandsBlock,
            939 => Self::CREATE_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateOperatorCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyCommand {
    fn from_id(id: usize) -> Self {
        match id {
            940 => Self::AlterAnnotationValueStmt,
            941 => Self::CreateAnnotationValueStmt,
            942 => Self::CreateSimpleExtending,
            943 => Self::SetFieldStmt,
            944 => Self::UsingStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            945 => Self::LBRACE_CreatePropertyCommandsList_OptSemicolons_RBRACE,
            946 => Self::LBRACE_OptSemicolons_RBRACE,
            947 => Self::LBRACE_Semicolons_CreatePropertyCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            948 => Self::CreatePropertyCommand,
            949 => Self::CreatePropertyCommandsList_Semicolons_CreatePropertyCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            951 => Self::CreatePropertySDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            952 => Self::CreateSimpleExtending,
            953 => Self::SetAnnotation,
            954 => Self::SetField,
            955 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            956 => Self::LBRACE_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
            957 => Self::LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandShort_RBRACE,
            958 => Self::LBRACE_OptSemicolons_CreatePropertySDLCommandsList_OptSemicolons_RBRACE,
            959 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertySDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            960 => Self::CreatePropertySDLCommandFull,
            961 => Self::CreatePropertySDLCommandsList_OptSemicolons_CreatePropertySDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            962 => Self::CREATE_ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_OptCreatePropertyCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            963 => Self::AlterAnnotationValueStmt,
            964 => Self::CreateAnnotationValueStmt,
            965 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            966 => Self::LBRACE_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
            967 => Self::LBRACE_OptSemicolons_RBRACE,
            968 => Self::LBRACE_Semicolons_CreatePseudoTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            969 => Self::CreatePseudoTypeCommand,
            970 => Self::CreatePseudoTypeCommandsList_Semicolons_CreatePseudoTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreatePseudoTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            971 => Self::CREATE_PSEUDO_TYPE_NodeName_OptCreatePseudoTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteCommand {
    fn from_id(id: usize) -> Self {
        match id {
            972 => Self::CreateAnnotationValueStmt,
            973 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            974 => Self::LBRACE_CreateRewriteCommandsList_OptSemicolons_RBRACE,
            975 => Self::LBRACE_OptSemicolons_RBRACE,
            976 => Self::LBRACE_Semicolons_CreateRewriteCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            977 => Self::CreateRewriteCommand,
            978 => Self::CreateRewriteCommandsList_Semicolons_CreateRewriteCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            980 => Self::CreateRewriteSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            981 => Self::SetAnnotation,
            982 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            983 => Self::LBRACE_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
            984 => Self::LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandShort_RBRACE,
            985 => Self::LBRACE_OptSemicolons_CreateRewriteSDLCommandsList_OptSemicolons_RBRACE,
            986 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            987 => Self::CreateRewriteSDLCommandFull,
            988 => Self::CreateRewriteSDLCommandsList_OptSemicolons_CreateRewriteSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRewriteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            989 => Self::CREATE_REWRITE_RewriteKindList_USING_ParenExpr_OptCreateRewriteCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleCommand {
    fn from_id(id: usize) -> Self {
        match id {
            990 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            991 => Self::LBRACE_CreateRoleCommandsList_OptSemicolons_RBRACE,
            992 => Self::LBRACE_OptSemicolons_RBRACE,
            993 => Self::LBRACE_Semicolons_CreateRoleCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            994 => Self::CreateRoleCommand,
            995 => Self::CreateRoleCommandsList_Semicolons_CreateRoleCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateRoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            996 => Self::CREATE_OptSuperuser_ROLE_ShortNodeName_OptShortExtending_OptIfNotExists_OptCreateRoleCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            998 => Self::CreateSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            999 => Self::SetAnnotation,
            1000 => Self::SetField,
            1001 => Self::Using,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1002 => Self::LBRACE_OptSemicolons_CreateSDLCommandShort_RBRACE,
            1003 => Self::LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_CreateSDLCommandShort_RBRACE,
            1004 => Self::LBRACE_OptSemicolons_CreateSDLCommandsList_OptSemicolons_RBRACE,
            1005 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1006 => Self::CreateSDLCommandFull,
            1007 => Self::CreateSDLCommandsList_OptSemicolons_CreateSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1008 => Self::AlterAnnotationValueStmt,
            1009 => Self::CreateAnnotationValueStmt,
            1010 => Self::CreateConcreteConstraintStmt,
            1011 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1012 => Self::LBRACE_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
            1013 => Self::LBRACE_OptSemicolons_RBRACE,
            1014 => Self::LBRACE_Semicolons_CreateScalarTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1015 => Self::CreateScalarTypeCommand,
            1016 => Self::CreateScalarTypeCommandsList_Semicolons_CreateScalarTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1017 => Self::ConcreteConstraintBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            1018 => Self::CreateScalarTypeSDLCommandBlock,
            1019 => Self::CreateScalarTypeSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            1020 => Self::ConcreteConstraintShort,
            1021 => Self::SetAnnotation,
            1022 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1023 => Self::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
            1024 => Self::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandShort_RBRACE,
            1025 => Self::LBRACE_OptSemicolons_CreateScalarTypeSDLCommandsList_OptSemicolons_RBRACE,
            1026 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1027 => Self::CreateScalarTypeSDLCommandFull,
            1028 => Self::CreateScalarTypeSDLCommandsList_OptSemicolons_CreateScalarTypeSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateScalarTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1029 => Self::CREATE_ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
            1030 => Self::CREATE_FINAL_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
            1031 => Self::CREATE_SCALAR_TYPE_NodeName_OptExtending_OptCreateScalarTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateSimpleExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1032 => Self::EXTENDING_SimpleTypeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1033 => Self::CreateAnnotationValueStmt,
            1034 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1035 => Self::LBRACE_CreateTriggerCommandsList_OptSemicolons_RBRACE,
            1036 => Self::LBRACE_OptSemicolons_RBRACE,
            1037 => Self::LBRACE_Semicolons_CreateTriggerCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1038 => Self::CreateTriggerCommand,
            1039 => Self::CreateTriggerCommandsList_Semicolons_CreateTriggerCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandFull {
    fn from_id(id: usize) -> Self {
        match id {
            1041 => Self::CreateTriggerSDLCommandShort_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandShort {
    fn from_id(id: usize) -> Self {
        match id {
            1042 => Self::SetAnnotation,
            1043 => Self::SetField,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1044 => Self::LBRACE_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
            1045 => Self::LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandShort_RBRACE,
            1046 => Self::LBRACE_OptSemicolons_CreateTriggerSDLCommandsList_OptSemicolons_RBRACE,
            1047 => Self::LBRACE_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerSDLCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1048 => Self::CreateTriggerSDLCommandFull,
            1049 => Self::CreateTriggerSDLCommandsList_OptSemicolons_CreateTriggerSDLCommandFull,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::CreateTriggerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1050 => Self::CREATE_TRIGGER_UnqualifiedPointerName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_OptCreateTriggerCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1051 => Self::BranchStmt,
            1052 => Self::DatabaseStmt,
            1053 => Self::ExtensionPackageStmt,
            1054 => Self::MigrationStmt,
            1055 => Self::OptWithDDLStmt,
            1056 => Self::RoleStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DDLWithBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1057 => Self::WithBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DatabaseName {
    fn from_id(id: usize) -> Self {
        match id {
            1058 => Self::Identifier,
            1059 => Self::ReservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1060 => Self::AlterDatabaseStmt,
            1061 => Self::CreateDatabaseStmt,
            1062 => Self::DropDatabaseStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DescribeFormat {
    fn from_id(id: usize) -> Self {
        match id {
            1063 => Self::AS_DDL,
            1064 => Self::AS_JSON,
            1065 => Self::AS_SDL,
            1066 => Self::AS_TEXT,
            1067 => Self::AS_TEXT_VERBOSE,
            1068 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DescribeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1069 => Self::DESCRIBE_CURRENT_BRANCH_CONFIG_DescribeFormat,
            1070 => Self::DESCRIBE_CURRENT_DATABASE_CONFIG_DescribeFormat,
            1071 => Self::DESCRIBE_CURRENT_MIGRATION_DescribeFormat,
            1072 => Self::DESCRIBE_INSTANCE_CONFIG_DescribeFormat,
            1073 => Self::DESCRIBE_OBJECT_NodeName_DescribeFormat,
            1074 => Self::DESCRIBE_ROLES_DescribeFormat,
            1075 => Self::DESCRIBE_SCHEMA_DescribeFormat,
            1076 => Self::DESCRIBE_SYSTEM_CONFIG_DescribeFormat,
            1077 => Self::DESCRIBE_SchemaItem_DescribeFormat,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DotName {
    fn from_id(id: usize) -> Self {
        match id {
            1078 => Self::DottedIdents,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DottedIdents {
    fn from_id(id: usize) -> Self {
        match id {
            1079 => Self::AnyIdentifier,
            1080 => Self::DottedIdents_DOT_AnyIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAccessPolicyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1081 => Self::DROP_ACCESS_POLICY_UnqualifiedPointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAliasStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1082 => Self::DROP_ALIAS_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAnnotationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1083 => Self::DROP_ABSTRACT_ANNOTATION_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropAnnotationValueStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1084 => Self::DROP_ANNOTATION_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropBranchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1085 => Self::DROP_BRANCH_DatabaseName_BranchOptions,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropCastStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1086 => Self::DROP_CAST_FROM_TypeName_TO_TypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1087 => Self::DROP_CONSTRAINT_NodeName_OptConcreteConstraintArgList_OptOnExpr_OptExceptExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1088 => Self::SetFieldStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1089 => Self::LBRACE_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
            1090 => Self::LBRACE_OptSemicolons_RBRACE,
            1091 => Self::LBRACE_Semicolons_DropConcreteIndexCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1092 => Self::DropConcreteIndexCommand,
            1093 => Self::DropConcreteIndexCommandsList_Semicolons_DropConcreteIndexCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1094 => Self::DROP_INDEX_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
            1095 => Self::DROP_INDEX_NodeName_OptIndexExtArgList_OnExpr_OptExceptExpr_OptDropConcreteIndexCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1096 => Self::DropConcreteConstraintStmt,
            1097 => Self::DropConcreteIndexStmt,
            1098 => Self::DropConcretePropertyStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1099 => Self::LBRACE_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
            1100 => Self::LBRACE_OptSemicolons_RBRACE,
            1101 => Self::LBRACE_Semicolons_DropConcreteLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1102 => Self::DropConcreteLinkCommand,
            1103 => Self::DropConcreteLinkCommandsList_Semicolons_DropConcreteLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcreteLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1104 => Self::DROP_LINK_UnqualifiedPointerName_OptDropConcreteLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConcretePropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1105 => Self::DROP_PROPERTY_UnqualifiedPointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropConstraintStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1106 => Self::DROP_ABSTRACT_CONSTRAINT_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropDatabaseStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1107 => Self::DROP_DATABASE_DatabaseName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropExtensionPackageStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1108 => Self::DROP_EXTENSIONPACKAGE_ShortNodeName_ExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1109 => Self::DROP_EXTENSION_ShortNodeName_OptExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropFunctionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1110 => Self::DROP_FUNCTION_NodeName_CreateFunctionArgs,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropFutureStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1111 => Self::DROP_FUTURE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropGlobalStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1112 => Self::DROP_GLOBAL_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropIndexMatchStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1113 => Self::DROP_INDEX_MATCH_FOR_TypeName_USING_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropIndexStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1114 => Self::DROP_ABSTRACT_INDEX_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1115 => Self::DropConcreteConstraintStmt,
            1116 => Self::DropConcreteIndexStmt,
            1117 => Self::DropConcretePropertyStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1118 => Self::LBRACE_DropLinkCommandsList_OptSemicolons_RBRACE,
            1119 => Self::LBRACE_OptSemicolons_RBRACE,
            1120 => Self::LBRACE_Semicolons_DropLinkCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1121 => Self::DropLinkCommand,
            1122 => Self::DropLinkCommandsList_Semicolons_DropLinkCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropLinkStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1123 => Self::DROP_ABSTRACT_LINK_PtrNodeName_OptDropLinkCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1124 => Self::DROP_MIGRATION_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropModuleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1125 => Self::DROP_MODULE_ModuleName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeCommand {
    fn from_id(id: usize) -> Self {
        match id {
            1126 => Self::DropConcreteConstraintStmt,
            1127 => Self::DropConcreteIndexStmt,
            1128 => Self::DropConcreteLinkStmt,
            1129 => Self::DropConcretePropertyStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1130 => Self::LBRACE_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
            1131 => Self::LBRACE_OptSemicolons_RBRACE,
            1132 => Self::LBRACE_Semicolons_DropObjectTypeCommandsList_OptSemicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeCommandsList {
    fn from_id(id: usize) -> Self {
        match id {
            1133 => Self::DropObjectTypeCommand,
            1134 => Self::DropObjectTypeCommandsList_Semicolons_DropObjectTypeCommand,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropObjectTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1135 => Self::DROP_TYPE_NodeName_OptDropObjectTypeCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropOperatorStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1136 => Self::DROP_OperatorKind_OPERATOR_NodeName_CreateFunctionArgs,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropPropertyStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1137 => Self::DROP_ABSTRACT_PROPERTY_PtrNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropRewriteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1138 => Self::DROP_REWRITE_RewriteKindList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropRoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1139 => Self::DROP_ROLE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropScalarTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1140 => Self::DROP_SCALAR_TYPE_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::DropTriggerStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1141 => Self::DROP_TRIGGER_UnqualifiedPointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::EdgeQLBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1142 => Self::OptSemicolons,
            1143 => Self::StatementBlock_OptSemicolons,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::EdgeQLGrammar {
    fn from_id(id: usize) -> Self {
        match id {
            1144 => Self::STARTBLOCK_EdgeQLBlock_EOI,
            1145 => Self::STARTEXTENSION_CreateExtensionPackageCommandsBlock_EOI,
            1146 => Self::STARTFRAGMENT_ExprStmt_EOI,
            1147 => Self::STARTFRAGMENT_Expr_EOI,
            1148 => Self::STARTMIGRATION_CreateMigrationCommandsBlock_EOI,
            1149 => Self::STARTSDLDOCUMENT_SDLDocument_EOI,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Expr {
    fn from_id(id: usize) -> Self {
        match id {
            1150 => Self::BaseAtomicExpr,
            1151 => Self::DETACHED_Expr,
            1152 => Self::DISTINCT_Expr,
            1153 => Self::EXISTS_Expr,
            1154 => Self::Expr_AND_Expr,
            1155 => Self::Expr_CIRCUMFLEX_Expr,
            1156 => Self::Expr_CompareOp_Expr_P_COMPARE_OP,
            1157 => Self::Expr_DOUBLEPLUS_Expr,
            1158 => Self::Expr_DOUBLEQMARK_Expr_P_DOUBLEQMARK_OP,
            1159 => Self::Expr_DOUBLESLASH_Expr,
            1160 => Self::Expr_EXCEPT_Expr,
            1161 => Self::Expr_IF_Expr_ELSE_Expr,
            1162 => Self::Expr_ILIKE_Expr,
            1163 => Self::Expr_INTERSECT_Expr,
            1164 => Self::Expr_IN_Expr,
            1165 => Self::Expr_IS_NOT_TypeExpr_P_IS,
            1166 => Self::Expr_IS_TypeExpr,
            1167 => Self::Expr_IndirectionEl,
            1168 => Self::Expr_LIKE_Expr,
            1169 => Self::Expr_MINUS_Expr,
            1170 => Self::Expr_NOT_ILIKE_Expr,
            1171 => Self::Expr_NOT_IN_Expr_P_IN,
            1172 => Self::Expr_NOT_LIKE_Expr,
            1173 => Self::Expr_OR_Expr,
            1174 => Self::Expr_PERCENT_Expr,
            1175 => Self::Expr_PLUS_Expr,
            1176 => Self::Expr_SLASH_Expr,
            1177 => Self::Expr_STAR_Expr,
            1178 => Self::Expr_Shape,
            1179 => Self::Expr_UNION_Expr,
            1180 => Self::GLOBAL_NodeName,
            1181 => Self::INTROSPECT_TypeExpr,
            1182 => Self::IfThenElseExpr,
            1183 => Self::LANGBRACKET_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
            1184 => Self::LANGBRACKET_OPTIONAL_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
            1185 => Self::LANGBRACKET_REQUIRED_FullTypeExpr_RANGBRACKET_Expr_P_TYPECAST,
            1186 => Self::MINUS_Expr_P_UMINUS,
            1187 => Self::NOT_Expr,
            1188 => Self::PLUS_Expr_P_UMINUS,
            1189 => Self::Path,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprList {
    fn from_id(id: usize) -> Self {
        match id {
            1190 => Self::ExprListInner,
            1191 => Self::ExprListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1192 => Self::Expr,
            1193 => Self::ExprListInner_COMMA_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1194 => Self::ExprStmtCore,
            1195 => Self::WithBlock_ExprStmtCore,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExprStmtCore {
    fn from_id(id: usize) -> Self {
        match id {
            1196 => Self::InternalGroup,
            1197 => Self::SimpleDelete,
            1198 => Self::SimpleFor,
            1199 => Self::SimpleGroup,
            1200 => Self::SimpleInsert,
            1201 => Self::SimpleSelect,
            1202 => Self::SimpleUpdate,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Extending {
    fn from_id(id: usize) -> Self {
        match id {
            1203 => Self::EXTENDING_TypeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtendingSimple {
    fn from_id(id: usize) -> Self {
        match id {
            1204 => Self::EXTENDING_SimpleTypeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionPackageStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1205 => Self::CreateExtensionPackageStmt,
            1206 => Self::DropExtensionPackageStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionRequirementDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1207 => Self::USING_EXTENSION_ShortNodeName_OptExtensionVersion,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1208 => Self::CreateExtensionStmt,
            1209 => Self::DropExtensionStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ExtensionVersion {
    fn from_id(id: usize) -> Self {
        match id {
            1210 => Self::VERSION_BaseStringConstant,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FilterClause {
    fn from_id(id: usize) -> Self {
        match id {
            1211 => Self::FILTER_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeComputableShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1212 => Self::FreeSimpleShapePointer_ASSIGN_Expr,
            1213 => Self::MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
            1214 => Self::OPTIONAL_FreeSimpleShapePointer_ASSIGN_Expr,
            1215 => Self::OPTIONAL_MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
            1216 => Self::OPTIONAL_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
            1217 => Self::REQUIRED_FreeSimpleShapePointer_ASSIGN_Expr,
            1218 => Self::REQUIRED_MULTI_FreeSimpleShapePointer_ASSIGN_Expr,
            1219 => Self::REQUIRED_SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
            1220 => Self::SINGLE_FreeSimpleShapePointer_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeComputableShapePointerList {
    fn from_id(id: usize) -> Self {
        match id {
            1221 => Self::FreeComputableShapePointerListInner,
            1222 => Self::FreeComputableShapePointerListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeComputableShapePointerListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1223 => Self::FreeComputableShapePointer,
            1224 => Self::FreeComputableShapePointerListInner_COMMA_FreeComputableShapePointer,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeShape {
    fn from_id(id: usize) -> Self {
        match id {
            1225 => Self::LBRACE_FreeComputableShapePointerList_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeSimpleShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1226 => Self::FreeStepName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FreeStepName {
    fn from_id(id: usize) -> Self {
        match id {
            1227 => Self::DUNDERTYPE,
            1228 => Self::ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FromFunction {
    fn from_id(id: usize) -> Self {
        match id {
            1229 => Self::USING_Identifier_BaseStringConstant,
            1230 => Self::USING_Identifier_EXPRESSION,
            1231 => Self::USING_Identifier_FUNCTION_BaseStringConstant,
            1232 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FullTypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1233 => Self::FullTypeExpr_AMPER_FullTypeExpr,
            1234 => Self::FullTypeExpr_PIPE_FullTypeExpr,
            1235 => Self::LPAREN_FullTypeExpr_RPAREN,
            1236 => Self::TYPEOF_Expr,
            1237 => Self::TypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncApplication {
    fn from_id(id: usize) -> Self {
        match id {
            1238 => Self::NodeName_LPAREN_OptFuncArgList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1239 => Self::FuncArgListInner,
            1240 => Self::FuncArgListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncArgListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1241 => Self::FuncArgListInner_COMMA_FuncCallArg,
            1242 => Self::FuncCallArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncCallArg {
    fn from_id(id: usize) -> Self {
        match id {
            1243 => Self::FuncCallArgExpr_OptFilterClause_OptSortClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncCallArgExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1244 => Self::AnyIdentifier_ASSIGN_Expr,
            1245 => Self::Expr,
            1246 => Self::PARAMETER_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArg {
    fn from_id(id: usize) -> Self {
        match id {
            1247 => Self::OptParameterKind_FuncDeclArgName_OptDefault,
            1248 => Self::OptParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1249 => Self::FuncDeclArgListInner,
            1250 => Self::FuncDeclArgListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1251 => Self::FuncDeclArg,
            1252 => Self::FuncDeclArgListInner_COMMA_FuncDeclArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgName {
    fn from_id(id: usize) -> Self {
        match id {
            1253 => Self::Identifier,
            1254 => Self::PARAMETER,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncDeclArgs {
    fn from_id(id: usize) -> Self {
        match id {
            1255 => Self::FuncDeclArgList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FuncExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1256 => Self::FuncApplication,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FunctionDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1257 => Self::FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FunctionDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1258 => Self::FUNCTION_NodeName_CreateFunctionArgs_ARROW_OptTypeQualifier_FunctionType_CreateFunctionSingleSDLCommandBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FunctionType {
    fn from_id(id: usize) -> Self {
        match id {
            1259 => Self::FullTypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FutureRequirementDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1260 => Self::USING_FUTURE_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::FutureStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1261 => Self::CreateFutureStmt,
            1262 => Self::DropFutureStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GlobalDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1263 => Self::GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
            1264 => Self::PtrQuals_GLOBAL_NodeName_OptPtrTarget_CreateGlobalSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GlobalDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1265 => Self::GLOBAL_NodeName_ASSIGN_Expr,
            1266 => Self::PtrQuals_GLOBAL_NodeName_ASSIGN_Expr,
            1267 => Self::GLOBAL_NodeName_PtrTarget,
            1268 => Self::PtrQuals_GLOBAL_NodeName_PtrTarget,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingAtom {
    fn from_id(id: usize) -> Self {
        match id {
            1269 => Self::GroupingIdent,
            1270 => Self::LPAREN_GroupingIdentList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingAtomList {
    fn from_id(id: usize) -> Self {
        match id {
            1271 => Self::GroupingAtomListInner,
            1272 => Self::GroupingAtomListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingAtomListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1273 => Self::GroupingAtom,
            1274 => Self::GroupingAtomListInner_COMMA_GroupingAtom,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingElement {
    fn from_id(id: usize) -> Self {
        match id {
            1275 => Self::CUBE_LPAREN_GroupingAtomList_RPAREN,
            1276 => Self::GroupingAtom,
            1277 => Self::LBRACE_GroupingElementList_RBRACE,
            1278 => Self::ROLLUP_LPAREN_GroupingAtomList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingElementList {
    fn from_id(id: usize) -> Self {
        match id {
            1279 => Self::GroupingElementListInner,
            1280 => Self::GroupingElementListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingElementListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1281 => Self::GroupingElement,
            1282 => Self::GroupingElementListInner_COMMA_GroupingElement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingIdent {
    fn from_id(id: usize) -> Self {
        match id {
            1283 => Self::AT_Identifier,
            1284 => Self::DOT_Identifier,
            1285 => Self::Identifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::GroupingIdentList {
    fn from_id(id: usize) -> Self {
        match id {
            1286 => Self::GroupingIdent,
            1287 => Self::GroupingIdentList_COMMA_GroupingIdent,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Identifier {
    fn from_id(id: usize) -> Self {
        match id {
            1288 => Self::IDENT,
            1289 => Self::UnreservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IfThenElseExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1290 => Self::IF_Expr_THEN_Expr_ELSE_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexArg {
    fn from_id(id: usize) -> Self {
        match id {
            1291 => Self::AnyIdentifier_ASSIGN_Expr,
            1292 => Self::FuncDeclArgName_OptDefault,
            1293 => Self::FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
            1294 => Self::ParameterKind_FuncDeclArgName_COLON_OptTypeQualifier_FullTypeExpr_OptDefault,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1295 => Self::IndexArgListInner,
            1296 => Self::IndexArgListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexArgListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1297 => Self::IndexArg,
            1298 => Self::IndexArgListInner_COMMA_IndexArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1299 => Self::ABSTRACT_INDEX_NodeName_OptExtendingSimple_CreateIndexSDLCommandsBlock,
            1300 => Self::ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple_CreateIndexSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1301 => Self::ABSTRACT_INDEX_NodeName_OptExtendingSimple,
            1302 => Self::ABSTRACT_INDEX_NodeName_IndexExtArgList_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndexExtArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1303 => Self::LPAREN_OptIndexArgList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::IndirectionEl {
    fn from_id(id: usize) -> Self {
        match id {
            1304 => Self::LBRACKET_COLON_Expr_RBRACKET,
            1305 => Self::LBRACKET_Expr_COLON_Expr_RBRACKET,
            1306 => Self::LBRACKET_Expr_COLON_RBRACKET,
            1307 => Self::LBRACKET_Expr_RBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::InnerDDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1308 => Self::AlterAliasStmt,
            1309 => Self::AlterAnnotationStmt,
            1310 => Self::AlterCastStmt,
            1311 => Self::AlterConstraintStmt,
            1312 => Self::AlterFunctionStmt,
            1313 => Self::AlterGlobalStmt,
            1314 => Self::AlterIndexStmt,
            1315 => Self::AlterLinkStmt,
            1316 => Self::AlterModuleStmt,
            1317 => Self::AlterObjectTypeStmt,
            1318 => Self::AlterOperatorStmt,
            1319 => Self::AlterPropertyStmt,
            1320 => Self::AlterScalarTypeStmt,
            1321 => Self::CreateAliasStmt,
            1322 => Self::CreateAnnotationStmt,
            1323 => Self::CreateCastStmt,
            1324 => Self::CreateConstraintStmt,
            1325 => Self::CreateFunctionStmt,
            1326 => Self::CreateGlobalStmt,
            1327 => Self::CreateIndexMatchStmt,
            1328 => Self::CreateIndexStmt,
            1329 => Self::CreateLinkStmt,
            1330 => Self::CreateModuleStmt,
            1331 => Self::CreateObjectTypeStmt,
            1332 => Self::CreateOperatorStmt,
            1333 => Self::CreatePropertyStmt,
            1334 => Self::CreatePseudoTypeStmt,
            1335 => Self::CreateScalarTypeStmt,
            1336 => Self::DropAliasStmt,
            1337 => Self::DropAnnotationStmt,
            1338 => Self::DropCastStmt,
            1339 => Self::DropConstraintStmt,
            1340 => Self::DropFunctionStmt,
            1341 => Self::DropGlobalStmt,
            1342 => Self::DropIndexMatchStmt,
            1343 => Self::DropIndexStmt,
            1344 => Self::DropLinkStmt,
            1345 => Self::DropModuleStmt,
            1346 => Self::DropObjectTypeStmt,
            1347 => Self::DropOperatorStmt,
            1348 => Self::DropPropertyStmt,
            1349 => Self::DropScalarTypeStmt,
            1350 => Self::ExtensionStmt,
            1351 => Self::FutureStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::InternalGroup {
    fn from_id(id: usize) -> Self {
        match id {
            1352 => Self::FOR_GROUP_OptionallyAliasedExpr_UsingClause_ByClause_IN_Identifier_OptGroupingAlias_UNION_OptionallyAliasedExpr_OptFilterClause_OptSortClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::LimitClause {
    fn from_id(id: usize) -> Self {
        match id {
            1353 => Self::LIMIT_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::LinkDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1354 => Self::ABSTRACT_LINK_PtrNodeName_OptExtendingSimple_CreateLinkSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::LinkDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1355 => Self::ABSTRACT_LINK_PtrNodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::MigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1356 => Self::AbortMigrationStmt,
            1357 => Self::AlterCurrentMigrationStmt,
            1358 => Self::AlterMigrationStmt,
            1359 => Self::CommitMigrationStmt,
            1360 => Self::CreateMigrationStmt,
            1361 => Self::DropMigrationStmt,
            1362 => Self::PopulateMigrationStmt,
            1363 => Self::ResetSchemaStmt,
            1364 => Self::StartMigrationStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ModuleDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1365 => Self::MODULE_ModuleName_SDLCommandBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ModuleName {
    fn from_id(id: usize) -> Self {
        match id {
            1366 => Self::DotName,
            1367 => Self::ModuleName_DOUBLECOLON_DotName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTuple {
    fn from_id(id: usize) -> Self {
        match id {
            1368 => Self::LPAREN_NamedTupleElementList_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTupleElement {
    fn from_id(id: usize) -> Self {
        match id {
            1369 => Self::ShortNodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTupleElementList {
    fn from_id(id: usize) -> Self {
        match id {
            1370 => Self::NamedTupleElementListInner,
            1371 => Self::NamedTupleElementListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NamedTupleElementListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1372 => Self::NamedTupleElement,
            1373 => Self::NamedTupleElementListInner_COMMA_NamedTupleElement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NestedQLBlockStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1374 => Self::OptWithDDLStmt,
            1375 => Self::SetFieldStmt,
            1376 => Self::Stmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1377 => Self::BaseName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::NontrivialTypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1379 => Self::LPAREN_FullTypeExpr_RPAREN,
            1380 => Self::TYPEOF_Expr,
            1381 => Self::TypeExpr_AMPER_TypeExpr,
            1382 => Self::TypeExpr_PIPE_TypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ObjectTypeDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1383 => Self::ABSTRACT_TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
            1384 => Self::TYPE_NodeName_OptExtendingSimple_CreateObjectTypeSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ObjectTypeDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1385 => Self::ABSTRACT_TYPE_NodeName_OptExtendingSimple,
            1386 => Self::TYPE_NodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OffsetClause {
    fn from_id(id: usize) -> Self {
        match id {
            1387 => Self::OFFSET_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1388 => Self::ON_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnSourceDeleteResetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1389 => Self::RESET_ON_SOURCE_DELETE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnSourceDeleteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1390 => Self::ON_SOURCE_DELETE_ALLOW,
            1391 => Self::ON_SOURCE_DELETE_DELETE_TARGET,
            1392 => Self::ON_SOURCE_DELETE_DELETE_TARGET_IF_ORPHAN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnTargetDeleteResetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1393 => Self::RESET_ON_TARGET_DELETE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OnTargetDeleteStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1394 => Self::ON_TARGET_DELETE_ALLOW,
            1395 => Self::ON_TARGET_DELETE_DEFERRED_RESTRICT,
            1396 => Self::ON_TARGET_DELETE_DELETE_SOURCE,
            1397 => Self::ON_TARGET_DELETE_RESTRICT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OperatorCode {
    fn from_id(id: usize) -> Self {
        match id {
            1398 => Self::USING_Identifier_BaseStringConstant,
            1399 => Self::USING_Identifier_EXPRESSION,
            1400 => Self::USING_Identifier_FUNCTION_BaseStringConstant,
            1401 => Self::USING_Identifier_OPERATOR_BaseStringConstant,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OperatorKind {
    fn from_id(id: usize) -> Self {
        match id {
            1402 => Self::INFIX,
            1403 => Self::POSTFIX,
            1404 => Self::PREFIX,
            1405 => Self::TERNARY,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptAlterUsingClause {
    fn from_id(id: usize) -> Self {
        match id {
            1406 => Self::USING_ParenExpr,
            1407 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptAnySubShape {
    fn from_id(id: usize) -> Self {
        match id {
            1408 => Self::COLON_Shape,
            1409 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptConcreteConstraintArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1410 => Self::LPAREN_OptPosCallArgList_RPAREN,
            1411 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateAccessPolicyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1412 => Self::CreateAccessPolicyCommandsBlock,
            1413 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateAnnotationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1414 => Self::CreateAnnotationCommandsBlock,
            1415 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1416 => Self::CreateCommandsBlock,
            1417 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1418 => Self::CreateConcreteLinkCommandsBlock,
            1419 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateConcretePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1420 => Self::CreateConcretePropertyCommandsBlock,
            1421 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateDatabaseCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1422 => Self::CreateDatabaseCommandsBlock,
            1423 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateExtensionCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1424 => Self::CreateExtensionCommandsBlock,
            1425 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateExtensionPackageCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1426 => Self::CreateExtensionPackageCommandsBlock,
            1427 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateGlobalCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1428 => Self::CreateGlobalCommandsBlock,
            1429 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1430 => Self::CreateIndexCommandsBlock,
            1431 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateIndexMatchCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1432 => Self::CreateIndexMatchCommandsBlock,
            1433 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1434 => Self::CreateLinkCommandsBlock,
            1435 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateMigrationCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1436 => Self::CreateMigrationCommandsBlock,
            1437 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1438 => Self::CreateObjectTypeCommandsBlock,
            1439 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateOperatorCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1440 => Self::CreateOperatorCommandsBlock,
            1441 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreatePropertyCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1442 => Self::CreatePropertyCommandsBlock,
            1443 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreatePseudoTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1444 => Self::CreatePseudoTypeCommandsBlock,
            1445 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateRewriteCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1446 => Self::CreateRewriteCommandsBlock,
            1447 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateRoleCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1448 => Self::CreateRoleCommandsBlock,
            1449 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateScalarTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1450 => Self::CreateScalarTypeCommandsBlock,
            1451 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptCreateTriggerCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1452 => Self::CreateTriggerCommandsBlock,
            1453 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDefault {
    fn from_id(id: usize) -> Self {
        match id {
            1454 => Self::EQUALS_Expr,
            1455 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDeferred {
    fn from_id(id: usize) -> Self {
        match id {
            1456 => Self::DEFERRED,
            1457 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDelegated {
    fn from_id(id: usize) -> Self {
        match id {
            1458 => Self::DELEGATED,
            1459 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDirection {
    fn from_id(id: usize) -> Self {
        match id {
            1460 => Self::ASC,
            1461 => Self::DESC,
            1462 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropConcreteIndexCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1463 => Self::DropConcreteIndexCommandsBlock,
            1464 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropConcreteLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1465 => Self::DropConcreteLinkCommandsBlock,
            1466 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropLinkCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1467 => Self::DropLinkCommandsBlock,
            1468 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptDropObjectTypeCommandsBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1469 => Self::DropObjectTypeCommandsBlock,
            1470 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExceptExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1471 => Self::EXCEPT_ParenExpr,
            1472 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExprList {
    fn from_id(id: usize) -> Self {
        match id {
            1473 => Self::ExprList,
            1474 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1475 => Self::Extending,
            1476 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExtendingSimple {
    fn from_id(id: usize) -> Self {
        match id {
            1477 => Self::ExtendingSimple,
            1478 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptExtensionVersion {
    fn from_id(id: usize) -> Self {
        match id {
            1479 => Self::ExtensionVersion,
            1480 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptFilterClause {
    fn from_id(id: usize) -> Self {
        match id {
            1481 => Self::FilterClause,
            1482 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptFuncArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1483 => Self::FuncArgList,
            1484 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptGroupingAlias {
    fn from_id(id: usize) -> Self {
        match id {
            1485 => Self::COMMA_Identifier,
            1486 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptIfNotExists {
    fn from_id(id: usize) -> Self {
        match id {
            1487 => Self::IF_NOT_EXISTS,
            1488 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptIndexArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1489 => Self::IndexArgList,
            1490 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptIndexExtArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1491 => Self::IndexExtArgList,
            1492 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptMigrationNameParentName {
    fn from_id(id: usize) -> Self {
        match id {
            1493 => Self::ShortNodeName,
            1494 => Self::ShortNodeName_ONTO_ShortNodeName,
            1495 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptNonesOrder {
    fn from_id(id: usize) -> Self {
        match id {
            1496 => Self::EMPTY_FIRST,
            1497 => Self::EMPTY_LAST,
            1498 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptOnExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1499 => Self::OnExpr,
            1500 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptParameterKind {
    fn from_id(id: usize) -> Self {
        match id {
            1501 => Self::ParameterKind,
            1502 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPosCallArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1503 => Self::PosCallArgList,
            1504 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPosition {
    fn from_id(id: usize) -> Self {
        match id {
            1505 => Self::AFTER_NodeName,
            1506 => Self::BEFORE_NodeName,
            1507 => Self::FIRST,
            1508 => Self::LAST,
            1509 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPtrQuals {
    fn from_id(id: usize) -> Self {
        match id {
            1510 => Self::PtrQuals,
            1511 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptPtrTarget {
    fn from_id(id: usize) -> Self {
        match id {
            1512 => Self::PtrTarget,
            1513 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSelectLimit {
    fn from_id(id: usize) -> Self {
        match id {
            1514 => Self::SelectLimit,
            1515 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSemicolons {
    fn from_id(id: usize) -> Self {
        match id {
            1516 => Self::Semicolons,
            1517 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptShortExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1518 => Self::ShortExtending,
            1519 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSortClause {
    fn from_id(id: usize) -> Self {
        match id {
            1520 => Self::SortClause,
            1521 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptSuperuser {
    fn from_id(id: usize) -> Self {
        match id {
            1522 => Self::SUPERUSER,
            1523 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptTransactionModeList {
    fn from_id(id: usize) -> Self {
        match id {
            1524 => Self::TransactionModeList,
            1525 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptTypeIntersection {
    fn from_id(id: usize) -> Self {
        match id {
            1526 => Self::TypeIntersection,
            1527 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptTypeQualifier {
    fn from_id(id: usize) -> Self {
        match id {
            1528 => Self::OPTIONAL,
            1529 => Self::SET_OF,
            1530 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptUnlessConflictClause {
    fn from_id(id: usize) -> Self {
        match id {
            1531 => Self::UnlessConflictCause,
            1532 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptUsingBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1533 => Self::USING_ParenExpr,
            1534 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptUsingClause {
    fn from_id(id: usize) -> Self {
        match id {
            1535 => Self::UsingClause,
            1536 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptWhenBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1537 => Self::WHEN_ParenExpr,
            1538 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptWithDDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1539 => Self::DDLWithBlock_WithDDLStmt,
            1540 => Self::WithDDLStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptionalOptional {
    fn from_id(id: usize) -> Self {
        match id {
            1541 => Self::OPTIONAL,
            1542 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OptionallyAliasedExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1543 => Self::AliasedExpr,
            1544 => Self::Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OrderbyExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1545 => Self::Expr_OptDirection_OptNonesOrder,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::OrderbyList {
    fn from_id(id: usize) -> Self {
        match id {
            1546 => Self::OrderbyExpr,
            1547 => Self::OrderbyList_THEN_OrderbyExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ParameterKind {
    fn from_id(id: usize) -> Self {
        match id {
            1548 => Self::NAMEDONLY,
            1549 => Self::VARIADIC,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ParenExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1550 => Self::LPAREN_ExprStmt_RPAREN,
            1551 => Self::LPAREN_Expr_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ParenTypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1552 => Self::LPAREN_FullTypeExpr_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PartialReservedKeyword {
    fn from_id(id: usize) -> Self {
        match id {
            1553 => Self::EXCEPT,
            1554 => Self::INTERSECT,
            1555 => Self::UNION,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Path {
    fn from_id(id: usize) -> Self {
        match id {
            1556 => Self::Expr_PathStep_P_DOT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PathNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1557 => Self::PtrIdentifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PathStep {
    fn from_id(id: usize) -> Self {
        match id {
            1558 => Self::AT_PathNodeName,
            1559 => Self::DOTBW_PathStepName,
            1560 => Self::DOT_ICONST,
            1561 => Self::DOT_PathStepName,
            1562 => Self::TypeIntersection,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PathStepName {
    fn from_id(id: usize) -> Self {
        match id {
            1563 => Self::DUNDERTYPE,
            1564 => Self::PathNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PointerName {
    fn from_id(id: usize) -> Self {
        match id {
            1565 => Self::DUNDERTYPE,
            1566 => Self::PtrNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PopulateMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1567 => Self::POPULATE_MIGRATION,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PosCallArg {
    fn from_id(id: usize) -> Self {
        match id {
            1568 => Self::Expr_OptFilterClause_OptSortClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PosCallArgList {
    fn from_id(id: usize) -> Self {
        match id {
            1569 => Self::PosCallArg,
            1570 => Self::PosCallArgList_COMMA_PosCallArg,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PropertyDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1571 => Self::ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple_CreatePropertySDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PropertyDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1572 => Self::ABSTRACT_PROPERTY_PtrNodeName_OptExtendingSimple,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrIdentifier {
    fn from_id(id: usize) -> Self {
        match id {
            1573 => Self::Identifier,
            1574 => Self::PartialReservedKeyword,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrName {
    fn from_id(id: usize) -> Self {
        match id {
            1575 => Self::PtrIdentifier,
            1576 => Self::QualifiedName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1577 => Self::PtrName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrQualifiedNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1578 => Self::QualifiedName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrQuals {
    fn from_id(id: usize) -> Self {
        match id {
            1579 => Self::MULTI,
            1580 => Self::OPTIONAL,
            1581 => Self::OPTIONAL_MULTI,
            1582 => Self::OPTIONAL_SINGLE,
            1583 => Self::REQUIRED,
            1584 => Self::REQUIRED_MULTI,
            1585 => Self::REQUIRED_SINGLE,
            1586 => Self::SINGLE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::PtrTarget {
    fn from_id(id: usize) -> Self {
        match id {
            1587 => Self::ARROW_FullTypeExpr,
            1588 => Self::COLON_FullTypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::QualifiedName {
    fn from_id(id: usize) -> Self {
        match id {
            1589 => Self::DUNDERSTD_DOUBLECOLON_ColonedIdents,
            1590 => Self::Identifier_DOUBLECOLON_ColonedIdents,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RenameStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1591 => Self::RENAME_TO_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ReservedKeyword {
    fn from_id(id: usize) -> Self {
        match id {
            1592 => Self::ADMINISTER,
            1593 => Self::ALTER,
            1594 => Self::ANALYZE,
            1595 => Self::AND,
            1596 => Self::ANYARRAY,
            1597 => Self::ANYOBJECT,
            1598 => Self::ANYTUPLE,
            1599 => Self::ANYTYPE,
            1600 => Self::BEGIN,
            1601 => Self::BY,
            1602 => Self::CASE,
            1603 => Self::CHECK,
            1604 => Self::COMMIT,
            1605 => Self::CONFIGURE,
            1606 => Self::CREATE,
            1607 => Self::DEALLOCATE,
            1608 => Self::DELETE,
            1609 => Self::DESCRIBE,
            1610 => Self::DETACHED,
            1611 => Self::DISCARD,
            1612 => Self::DISTINCT,
            1613 => Self::DO,
            1614 => Self::DROP,
            1615 => Self::DUNDERDEFAULT,
            1616 => Self::DUNDEREDGEDBSYS,
            1617 => Self::DUNDEREDGEDBTPL,
            1618 => Self::DUNDERNEW,
            1619 => Self::DUNDEROLD,
            1620 => Self::DUNDERSOURCE,
            1621 => Self::DUNDERSPECIFIED,
            1622 => Self::DUNDERSTD,
            1623 => Self::DUNDERSUBJECT,
            1624 => Self::DUNDERTYPE,
            1625 => Self::ELSE,
            1626 => Self::END,
            1627 => Self::EXISTS,
            1628 => Self::EXPLAIN,
            1629 => Self::EXTENDING,
            1630 => Self::FALSE,
            1631 => Self::FETCH,
            1632 => Self::FILTER,
            1633 => Self::FOR,
            1634 => Self::GET,
            1635 => Self::GLOBAL,
            1636 => Self::GRANT,
            1637 => Self::GROUP,
            1638 => Self::IF,
            1639 => Self::ILIKE,
            1640 => Self::IMPORT,
            1641 => Self::IN,
            1642 => Self::INSERT,
            1643 => Self::INTROSPECT,
            1644 => Self::IS,
            1645 => Self::LIKE,
            1646 => Self::LIMIT,
            1647 => Self::LISTEN,
            1648 => Self::LOAD,
            1649 => Self::LOCK,
            1650 => Self::MATCH,
            1651 => Self::MODULE,
            1652 => Self::MOVE,
            1653 => Self::NEVER,
            1654 => Self::NOT,
            1655 => Self::NOTIFY,
            1656 => Self::OFFSET,
            1657 => Self::ON,
            1658 => Self::OPTIONAL,
            1659 => Self::OR,
            1660 => Self::OVER,
            1661 => Self::PARTITION,
            1662 => Self::PREPARE,
            1663 => Self::RAISE,
            1664 => Self::REFRESH,
            1665 => Self::REVOKE,
            1666 => Self::ROLLBACK,
            1667 => Self::SELECT,
            1668 => Self::SET,
            1669 => Self::SINGLE,
            1670 => Self::START,
            1671 => Self::TRUE,
            1672 => Self::TYPEOF,
            1673 => Self::UPDATE,
            1674 => Self::VARIADIC,
            1675 => Self::WHEN,
            1676 => Self::WINDOW,
            1677 => Self::WITH,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ResetFieldStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1678 => Self::RESET_DEFAULT,
            1679 => Self::RESET_IDENT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ResetSchemaStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1680 => Self::RESET_SCHEMA_TO_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ResetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1681 => Self::RESET_ALIAS_Identifier,
            1682 => Self::RESET_ALIAS_STAR,
            1683 => Self::RESET_MODULE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1684 => Self::REWRITE_RewriteKindList_USING_ParenExpr_CreateRewriteSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1685 => Self::REWRITE_RewriteKindList_USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteKind {
    fn from_id(id: usize) -> Self {
        match id {
            1686 => Self::INSERT,
            1687 => Self::UPDATE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RewriteKindList {
    fn from_id(id: usize) -> Self {
        match id {
            1688 => Self::RewriteKind,
            1689 => Self::RewriteKindList_COMMA_RewriteKind,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::RoleStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1690 => Self::AlterRoleStmt,
            1691 => Self::CreateRoleStmt,
            1692 => Self::DropRoleStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLBlockStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1693 => Self::AliasDeclaration,
            1694 => Self::AnnotationDeclaration,
            1695 => Self::ConstraintDeclaration,
            1696 => Self::FunctionDeclaration,
            1697 => Self::GlobalDeclaration,
            1698 => Self::IndexDeclaration,
            1699 => Self::LinkDeclaration,
            1700 => Self::ModuleDeclaration,
            1701 => Self::ObjectTypeDeclaration,
            1702 => Self::PropertyDeclaration,
            1703 => Self::ScalarTypeDeclaration,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLCommandBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1704 => Self::LBRACE_OptSemicolons_RBRACE,
            1705 => Self::LBRACE_OptSemicolons_SDLStatements_RBRACE,
            1706 => Self::LBRACE_OptSemicolons_SDLShortStatement_RBRACE,
            1707 => Self::LBRACE_OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement_RBRACE,
            1708 => Self::LBRACE_OptSemicolons_SDLStatements_Semicolons_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLDocument {
    fn from_id(id: usize) -> Self {
        match id {
            1709 => Self::OptSemicolons,
            1710 => Self::OptSemicolons_SDLStatements,
            1711 => Self::OptSemicolons_SDLStatements_Semicolons,
            1712 => Self::OptSemicolons_SDLShortStatement,
            1713 => Self::OptSemicolons_SDLStatements_OptSemicolons_SDLShortStatement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLShortStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1714 => Self::AliasDeclarationShort,
            1715 => Self::AnnotationDeclarationShort,
            1716 => Self::ConstraintDeclarationShort,
            1717 => Self::ExtensionRequirementDeclaration,
            1718 => Self::FunctionDeclarationShort,
            1719 => Self::FutureRequirementDeclaration,
            1720 => Self::GlobalDeclarationShort,
            1721 => Self::IndexDeclarationShort,
            1722 => Self::LinkDeclarationShort,
            1723 => Self::ObjectTypeDeclarationShort,
            1724 => Self::PropertyDeclarationShort,
            1725 => Self::ScalarTypeDeclarationShort,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1726 => Self::SDLBlockStatement,
            1727 => Self::SDLShortStatement_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SDLStatements {
    fn from_id(id: usize) -> Self {
        match id {
            1728 => Self::SDLStatement,
            1729 => Self::SDLStatements_OptSemicolons_SDLStatement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ScalarTypeDeclaration {
    fn from_id(id: usize) -> Self {
        match id {
            1730 => Self::ABSTRACT_SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
            1731 => Self::SCALAR_TYPE_NodeName_OptExtending_CreateScalarTypeSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ScalarTypeDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1732 => Self::ABSTRACT_SCALAR_TYPE_NodeName_OptExtending,
            1733 => Self::SCALAR_TYPE_NodeName_OptExtending,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SchemaItem {
    fn from_id(id: usize) -> Self {
        match id {
            1734 => Self::SchemaObjectClass_NodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SchemaObjectClass {
    fn from_id(id: usize) -> Self {
        match id {
            1735 => Self::ALIAS,
            1736 => Self::ANNOTATION,
            1737 => Self::CAST,
            1738 => Self::CONSTRAINT,
            1739 => Self::FUNCTION,
            1740 => Self::LINK,
            1741 => Self::MODULE,
            1742 => Self::OPERATOR,
            1743 => Self::PROPERTY,
            1744 => Self::SCALAR_TYPE,
            1745 => Self::TYPE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SelectLimit {
    fn from_id(id: usize) -> Self {
        match id {
            1746 => Self::LimitClause,
            1747 => Self::OffsetClause,
            1748 => Self::OffsetClause_LimitClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Semicolons {
    fn from_id(id: usize) -> Self {
        match id {
            1749 => Self::SEMICOLON,
            1750 => Self::Semicolons_SEMICOLON,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SessionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1751 => Self::ResetStmt,
            1752 => Self::SetStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Set {
    fn from_id(id: usize) -> Self {
        match id {
            1753 => Self::LBRACE_OptExprList_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetAnnotation {
    fn from_id(id: usize) -> Self {
        match id {
            1754 => Self::ANNOTATION_NodeName_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetCardinalityStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1755 => Self::RESET_CARDINALITY_OptAlterUsingClause,
            1756 => Self::SET_MULTI,
            1757 => Self::SET_SINGLE_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetDelegatedStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1758 => Self::RESET_DELEGATED,
            1759 => Self::SET_DELEGATED,
            1760 => Self::SET_NOT_DELEGATED,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetField {
    fn from_id(id: usize) -> Self {
        match id {
            1761 => Self::Identifier_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetFieldStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1762 => Self::SET_Identifier_ASSIGN_Expr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetGlobalTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1763 => Self::RESET_TYPE,
            1764 => Self::SETTYPE_FullTypeExpr_OptAlterUsingClause,
            1765 => Self::SETTYPE_FullTypeExpr_RESET_TO_DEFAULT,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetPointerTypeStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1766 => Self::RESET_TYPE,
            1767 => Self::SETTYPE_FullTypeExpr_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetRequiredInCreateStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1768 => Self::SET_REQUIRED_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetRequiredStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1769 => Self::DROP_REQUIRED,
            1770 => Self::RESET_OPTIONALITY,
            1771 => Self::SET_OPTIONAL,
            1772 => Self::SET_REQUIRED_OptAlterUsingClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SetStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1773 => Self::SET_ALIAS_Identifier_AS_MODULE_ModuleName,
            1774 => Self::SET_MODULE_ModuleName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Shape {
    fn from_id(id: usize) -> Self {
        match id {
            1775 => Self::LBRACE_RBRACE,
            1776 => Self::LBRACE_ShapeElementList_RBRACE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapeElement {
    fn from_id(id: usize) -> Self {
        match id {
            1777 => Self::ComputableShapePointer,
            1778 => Self::ShapePointer_OptAnySubShape_OptFilterClause_OptSortClause_OptSelectLimit,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapeElementList {
    fn from_id(id: usize) -> Self {
        match id {
            1779 => Self::ShapeElementListInner,
            1780 => Self::ShapeElementListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapeElementListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1781 => Self::ShapeElement,
            1782 => Self::ShapeElementListInner_COMMA_ShapeElement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapePath {
    fn from_id(id: usize) -> Self {
        match id {
            1783 => Self::AT_PathNodeName,
            1784 => Self::PathStepName_OptTypeIntersection,
            1785 => Self::Splat,
            1786 => Self::TypeIntersection_DOT_PathStepName_OptTypeIntersection,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1787 => Self::ShapePath,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShortExtending {
    fn from_id(id: usize) -> Self {
        match id {
            1788 => Self::EXTENDING_ShortNodeNameList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShortNodeName {
    fn from_id(id: usize) -> Self {
        match id {
            1789 => Self::Identifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::ShortNodeNameList {
    fn from_id(id: usize) -> Self {
        match id {
            1790 => Self::ShortNodeName,
            1791 => Self::ShortNodeNameList_COMMA_ShortNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleDelete {
    fn from_id(id: usize) -> Self {
        match id {
            1792 => Self::DELETE_Expr_OptFilterClause_OptSortClause_OptSelectLimit,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleFor {
    fn from_id(id: usize) -> Self {
        match id {
            1793 => Self::FOR_OptionalOptional_Identifier_IN_AtomicExpr_UNION_Expr,
            1794 => Self::FOR_OptionalOptional_Identifier_IN_AtomicExpr_ExprStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleGroup {
    fn from_id(id: usize) -> Self {
        match id {
            1795 => Self::GROUP_OptionallyAliasedExpr_OptUsingClause_ByClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleInsert {
    fn from_id(id: usize) -> Self {
        match id {
            1796 => Self::INSERT_Expr_OptUnlessConflictClause,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleSelect {
    fn from_id(id: usize) -> Self {
        match id {
            1797 => Self::SELECT_OptionallyAliasedExpr_OptFilterClause_OptSortClause_OptSelectLimit,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleShapePath {
    fn from_id(id: usize) -> Self {
        match id {
            1798 => Self::AT_PathNodeName,
            1799 => Self::PathStepName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleShapePointer {
    fn from_id(id: usize) -> Self {
        match id {
            1800 => Self::SimpleShapePath,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleTypeName {
    fn from_id(id: usize) -> Self {
        match id {
            1801 => Self::ANYOBJECT,
            1802 => Self::ANYTUPLE,
            1803 => Self::ANYTYPE,
            1804 => Self::PtrNodeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleTypeNameList {
    fn from_id(id: usize) -> Self {
        match id {
            1805 => Self::SimpleTypeName,
            1806 => Self::SimpleTypeNameList_COMMA_SimpleTypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SimpleUpdate {
    fn from_id(id: usize) -> Self {
        match id {
            1807 => Self::UPDATE_Expr_OptFilterClause_SET_Shape,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SingleStatement {
    fn from_id(id: usize) -> Self {
        match id {
            1808 => Self::ConfigStmt,
            1809 => Self::DDLStmt,
            1810 => Self::IfThenElseExpr,
            1811 => Self::SessionStmt,
            1812 => Self::Stmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SortClause {
    fn from_id(id: usize) -> Self {
        match id {
            1813 => Self::ORDERBY_OrderbyList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Splat {
    fn from_id(id: usize) -> Self {
        match id {
            1814 => Self::DOUBLESTAR,
            1815 => Self::ParenTypeExpr_DOT_DOUBLESTAR,
            1816 => Self::ParenTypeExpr_DOT_STAR,
            1817 => Self::ParenTypeExpr_TypeIntersection_DOT_DOUBLESTAR,
            1818 => Self::ParenTypeExpr_TypeIntersection_DOT_STAR,
            1819 => Self::PathStepName_DOT_DOUBLESTAR,
            1820 => Self::PathStepName_DOT_STAR,
            1821 => Self::PathStepName_TypeIntersection_DOT_DOUBLESTAR,
            1822 => Self::PathStepName_TypeIntersection_DOT_STAR,
            1823 => Self::PtrQualifiedNodeName_DOT_DOUBLESTAR,
            1824 => Self::PtrQualifiedNodeName_DOT_STAR,
            1825 => Self::PtrQualifiedNodeName_TypeIntersection_DOT_DOUBLESTAR,
            1826 => Self::PtrQualifiedNodeName_TypeIntersection_DOT_STAR,
            1827 => Self::STAR,
            1828 => Self::TypeIntersection_DOT_DOUBLESTAR,
            1829 => Self::TypeIntersection_DOT_STAR,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::StartMigrationStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1830 => Self::START_MIGRATION_TO_SDLCommandBlock,
            1831 => Self::START_MIGRATION_REWRITE,
            1832 => Self::START_MIGRATION_TO_COMMITTED_SCHEMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::StatementBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1833 => Self::SingleStatement,
            1834 => Self::StatementBlock_Semicolons_SingleStatement,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Stmt {
    fn from_id(id: usize) -> Self {
        match id {
            1835 => Self::AdministerStmt,
            1836 => Self::AnalyzeStmt,
            1837 => Self::DescribeStmt,
            1838 => Self::ExprStmt,
            1839 => Self::TransactionStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Subtype {
    fn from_id(id: usize) -> Self {
        match id {
            1840 => Self::BaseNumberConstant,
            1841 => Self::BaseStringConstant,
            1842 => Self::FullTypeExpr,
            1843 => Self::Identifier_COLON_FullTypeExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SubtypeList {
    fn from_id(id: usize) -> Self {
        match id {
            1844 => Self::SubtypeListInner,
            1845 => Self::SubtypeListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::SubtypeListInner {
    fn from_id(id: usize) -> Self {
        match id {
            1846 => Self::Subtype,
            1847 => Self::SubtypeListInner_COMMA_Subtype,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TransactionMode {
    fn from_id(id: usize) -> Self {
        match id {
            1848 => Self::DEFERRABLE,
            1849 => Self::ISOLATION_SERIALIZABLE,
            1850 => Self::NOT_DEFERRABLE,
            1851 => Self::READ_ONLY,
            1852 => Self::READ_WRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TransactionModeList {
    fn from_id(id: usize) -> Self {
        match id {
            1853 => Self::TransactionMode,
            1854 => Self::TransactionModeList_COMMA_TransactionMode,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TransactionStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1855 => Self::COMMIT,
            1856 => Self::DECLARE_SAVEPOINT_Identifier,
            1857 => Self::RELEASE_SAVEPOINT_Identifier,
            1858 => Self::ROLLBACK,
            1859 => Self::ROLLBACK_TO_SAVEPOINT_Identifier,
            1860 => Self::START_TRANSACTION_OptTransactionModeList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerDeclarationBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1861 => Self::TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr_CreateTriggerSDLCommandsBlock,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerDeclarationShort {
    fn from_id(id: usize) -> Self {
        match id {
            1862 => Self::TRIGGER_NodeName_TriggerTiming_TriggerKindList_FOR_TriggerScope_OptWhenBlock_DO_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerKind {
    fn from_id(id: usize) -> Self {
        match id {
            1863 => Self::DELETE,
            1864 => Self::INSERT,
            1865 => Self::UPDATE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerKindList {
    fn from_id(id: usize) -> Self {
        match id {
            1866 => Self::TriggerKind,
            1867 => Self::TriggerKindList_COMMA_TriggerKind,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerScope {
    fn from_id(id: usize) -> Self {
        match id {
            1868 => Self::ALL,
            1869 => Self::EACH,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TriggerTiming {
    fn from_id(id: usize) -> Self {
        match id {
            1870 => Self::AFTER,
            1871 => Self::AFTER_COMMIT_OF,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Tuple {
    fn from_id(id: usize) -> Self {
        match id {
            1872 => Self::LPAREN_Expr_COMMA_OptExprList_RPAREN,
            1873 => Self::LPAREN_RPAREN,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeExpr {
    fn from_id(id: usize) -> Self {
        match id {
            1874 => Self::NontrivialTypeExpr,
            1875 => Self::SimpleTypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeIntersection {
    fn from_id(id: usize) -> Self {
        match id {
            1876 => Self::LBRACKET_IS_FullTypeExpr_RBRACKET,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeName {
    fn from_id(id: usize) -> Self {
        match id {
            1877 => Self::CollectionTypeName,
            1878 => Self::SimpleTypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::TypeNameList {
    fn from_id(id: usize) -> Self {
        match id {
            1879 => Self::TypeName,
            1880 => Self::TypeNameList_COMMA_TypeName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnlessConflictCause {
    fn from_id(id: usize) -> Self {
        match id {
            1881 => Self::UNLESS_CONFLICT_UnlessConflictSpecifier,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnlessConflictSpecifier {
    fn from_id(id: usize) -> Self {
        match id {
            1882 => Self::ON_Expr,
            1883 => Self::ON_Expr_ELSE_Expr,
            1884 => Self::epsilon,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnqualifiedPointerName {
    fn from_id(id: usize) -> Self {
        match id {
            1885 => Self::PointerName,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UnreservedKeyword {
    fn from_id(id: usize) -> Self {
        match id {
            1886 => Self::ABORT,
            1887 => Self::ABSTRACT,
            1888 => Self::ACCESS,
            1889 => Self::AFTER,
            1890 => Self::ALIAS,
            1891 => Self::ALL,
            1892 => Self::ALLOW,
            1893 => Self::ANNOTATION,
            1894 => Self::APPLIED,
            1895 => Self::AS,
            1896 => Self::ASC,
            1897 => Self::ASSIGNMENT,
            1898 => Self::BEFORE,
            1899 => Self::BLOBAL,
            1900 => Self::BRANCH,
            1901 => Self::CARDINALITY,
            1902 => Self::CAST,
            1903 => Self::COMMITTED,
            1904 => Self::CONFIG,
            1905 => Self::CONFLICT,
            1906 => Self::CONSTRAINT,
            1907 => Self::CUBE,
            1908 => Self::CURRENT,
            1909 => Self::DATA,
            1910 => Self::DATABASE,
            1911 => Self::DDL,
            1912 => Self::DECLARE,
            1913 => Self::DEFAULT,
            1914 => Self::DEFERRABLE,
            1915 => Self::DEFERRED,
            1916 => Self::DELEGATED,
            1917 => Self::DENY,
            1918 => Self::DESC,
            1919 => Self::EACH,
            1920 => Self::EMPTY,
            1921 => Self::EXPRESSION,
            1922 => Self::EXTENSION,
            1923 => Self::FINAL,
            1924 => Self::FIRST,
            1925 => Self::FORCE,
            1926 => Self::FROM,
            1927 => Self::FUNCTION,
            1928 => Self::FUTURE,
            1929 => Self::IMPLICIT,
            1930 => Self::INDEX,
            1931 => Self::INFIX,
            1932 => Self::INHERITABLE,
            1933 => Self::INSTANCE,
            1934 => Self::INTO,
            1935 => Self::ISOLATION,
            1936 => Self::JSON,
            1937 => Self::LAST,
            1938 => Self::LINK,
            1939 => Self::MIGRATION,
            1940 => Self::MULTI,
            1941 => Self::NAMED,
            1942 => Self::OBJECT,
            1943 => Self::OF,
            1944 => Self::ONLY,
            1945 => Self::ONTO,
            1946 => Self::OPERATOR,
            1947 => Self::OPTIONALITY,
            1948 => Self::ORDER,
            1949 => Self::ORPHAN,
            1950 => Self::OVERLOADED,
            1951 => Self::OWNED,
            1952 => Self::PACKAGE,
            1953 => Self::POLICY,
            1954 => Self::POPULATE,
            1955 => Self::POSTFIX,
            1956 => Self::PREFIX,
            1957 => Self::PROPERTY,
            1958 => Self::PROPOSED,
            1959 => Self::PSEUDO,
            1960 => Self::READ,
            1961 => Self::REJECT,
            1962 => Self::RELEASE,
            1963 => Self::RENAME,
            1964 => Self::REQUIRED,
            1965 => Self::RESET,
            1966 => Self::RESTRICT,
            1967 => Self::REWRITE,
            1968 => Self::ROLE,
            1969 => Self::ROLES,
            1970 => Self::ROLLUP,
            1971 => Self::SAVEPOINT,
            1972 => Self::SCALAR,
            1973 => Self::SCHEMA,
            1974 => Self::SDL,
            1975 => Self::SERIALIZABLE,
            1976 => Self::SESSION,
            1977 => Self::SOURCE,
            1978 => Self::SUPERUSER,
            1979 => Self::SYSTEM,
            1980 => Self::TARGET,
            1981 => Self::TEMPLATE,
            1982 => Self::TERNARY,
            1983 => Self::TEXT,
            1984 => Self::THEN,
            1985 => Self::TO,
            1986 => Self::TRANSACTION,
            1987 => Self::TRIGGER,
            1988 => Self::TYPE,
            1989 => Self::UNLESS,
            1990 => Self::USING,
            1991 => Self::VERBOSE,
            1992 => Self::VERSION,
            1993 => Self::VIEW,
            1994 => Self::WRITE,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::Using {
    fn from_id(id: usize) -> Self {
        match id {
            1995 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UsingClause {
    fn from_id(id: usize) -> Self {
        match id {
            1996 => Self::USING_AliasedExprList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::UsingStmt {
    fn from_id(id: usize) -> Self {
        match id {
            1997 => Self::RESET_EXPRESSION,
            1998 => Self::USING_ParenExpr,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithBlock {
    fn from_id(id: usize) -> Self {
        match id {
            1999 => Self::WITH_WithDeclList,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDDLStmt {
    fn from_id(id: usize) -> Self {
        match id {
            2000 => Self::InnerDDLStmt,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDecl {
    fn from_id(id: usize) -> Self {
        match id {
            2001 => Self::AliasDecl,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDeclList {
    fn from_id(id: usize) -> Self {
        match id {
            2002 => Self::WithDeclListInner,
            2003 => Self::WithDeclListInner_COMMA,
          _ => unreachable!(),
        }
    }
}

impl super::FromId for super::WithDeclListInner {
    fn from_id(id: usize) -> Self {
        match id {
            2004 => Self::WithDecl,
            2005 => Self::WithDeclListInner_COMMA_WithDecl,
          _ => unreachable!(),
        }
    }
}
