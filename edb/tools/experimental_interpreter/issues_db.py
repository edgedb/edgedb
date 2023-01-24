

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
        101 : DBEntry(PriorityT, dict_to_val({"name" : [StrVal("High")]})),
        102 : DBEntry(PriorityT, dict_to_val({"name" : [StrVal("Low")]})),
        111 : DBEntry(StatusT, dict_to_val({"name" : [StrVal("Open")]})),
        112 : DBEntry(StatusT, dict_to_val({"name" : [StrVal("Closed")]})),
        121 : DBEntry(UserT, dict_to_val({"name" : [StrVal("Elvis")], "todo": []})),
        122 : DBEntry(UrlT, dict_to_val({"name" : [StrVal("edgedb.com")], "address": [StrVal('https://edgedb.com')]})),
        131 : DBEntry(FileT, dict_to_val({"name" : [StrVal("screenshot.png")]})),
        141 : DBEntry(LogEntryT, 
                                dict_to_val({"owner" : 
                                    [LinkWithPropertyVal(
                                            ref(121),
                                            RefLinkVal(141, dict_to_val({"note", StrVal("reassigned")})),
                                        )
                                    ]})),
    }
)

if __name__ == "__main__":
   pprint.pprint(issues_db_partial)