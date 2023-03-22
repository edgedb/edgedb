
from .data import data_ops as e

from typing import List, Any
def do_write_logs(logs: List[Any], filename: str):
    def format_entry(entry, index):
        result = "<a href='#' onclick='toggle({})'>Input/Output {}</a>\n".format(index, index)
        result += "<div class='entry' id='entry{}'>".format(index)
        result += "<div class='input'>{}</div>".format(entry[0])
        if len(entry) > 1:
            result += "<div class='output'>{}</div>".format(entry[1])
        result += "</div>\n"
        return result

    def format_log(log, index):
        result = "<ul>\n"
        for i, entry in enumerate(log):
            result += "<li>\n"
            if isinstance(entry, list):
                result += "<a href='#' onclick='toggle({})'>Log {}</a>\n".format(index + i, index + i)
                result += format_log(entry, index + i)
            else:
                result += format_entry(entry, index + i)
            result += "</li>\n"
        result += "</ul>\n"
        return result

    with open(filename, "w") as f:
        f.write("<html>\n")
        f.write("<head>\n")
        f.write("<title>Log</title>\n")
        f.write("<style>\n")
        f.write(".entry { margin-left: 20px; }\n")
        f.write("</style>\n")
        f.write("</head>\n")
        f.write("<body>\n")
        f.write("<h1>Log</h1>\n")
        f.write(format_log(logs, 0))
        f.write("<script>\n")
        f.write("function toggle(index) {\n")
        f.write("  var entry = document.getElementById('entry' + index);\n")
        f.write("  entry.style.display = entry.style.display === 'none' ? 'block' : 'none';\n")
        f.write("}\n")
        f.write("</script>\n")
        f.write("</body>\n")
        f.write("</html>\n")

def write_logs_to_file(logs: List[Any], filepath: str):
    # the logs are structured as follows:
    # Log ::= [(Input, Output), Log_1, ..., Log_n]
    # where Log_1 ... Log_n are sub logs
    do_write_logs(logs, filepath)
