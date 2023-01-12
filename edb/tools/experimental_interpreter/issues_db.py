

from data_ops import *

PriorityT = VarTp("Priority")
StatusT = VarTp("Status")
UserT = VarTp("User")
UrlT = VarTp("URL")


issues_db = DB(
    data={
        next_id() : DBEntry(PriorityT, {"name" : StrVal("High")}),
        next_id() : DBEntry(PriorityT, {"name" : StrVal("Low")}),
        next_id() : DBEntry(StatusT, {"name" : StrVal("Open")}),
        next_id() : DBEntry(StatusT, {"name" : StrVal("Closed")}),
        next_id() : DBEntry(UserT, {"name" : StrVal("Elvis"), "todo": EmptyUnionExpr}),
    }
)

if __name__ == "__main__":
    print(issues_db)