

from data_ops import *

import pprint

PriorityT = VarTp("Priority")
StatusT = VarTp("Status")
UserT = VarTp("User")
UrlT = VarTp("URL")
FileT = VarTp("File")
LogEntryT = VarTp("LogEntry")


issues_db_partial = DB(
    data={
        next_id() : DBEntry(PriorityT, {"name" : [StrVal("High")]}),
        next_id() : DBEntry(PriorityT, {"name" : [StrVal("Low")]}),
        next_id() : DBEntry(StatusT, {"name" : [StrVal("Open")]}),
        next_id() : DBEntry(StatusT, {"name" : [StrVal("Closed")]}),
        next_id() : DBEntry(UserT, {"name" : [StrVal("Elvis")], "todo": []}),
        next_id() : DBEntry(UrlT, {"name" : [StrVal("edgedb.com")], "address": [StrVal('https://edgedb.com')]}),
    }
)

if __name__ == "__main__":
   pprint.pprint(issues_db_partial)