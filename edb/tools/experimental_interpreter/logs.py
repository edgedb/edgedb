import json
from typing import List, Any
from edb.edgeql import codegen
from .back_to_ql import reverse_elab
from .data.val_to_json import multi_set_val_to_json_like
from .data import expr_to_str as pp


def to_html_str(s: str) -> str:
    return s.replace("λ", "&lambda;")


def do_write_logs(logs: List[Any], filename: str):

    def format_entry(entry, index):
        entry_id = '_'.join(map(str, index))
        result = """<a href='#/' onclick='toggle("{}")'>
                    Input/Output {}</a>\n""".format(
            entry_id, entry_id
        )
        result += """<button onclick='foldEntry(\"{}\")'>Fold</button>
                   <button onclick='unfoldEntry(\"{}\")'>
                   Unfold</button>""".format(
            entry_id, entry_id
        )
        result += "<div class='entry' id='entry_{}'>".format(entry_id)
        result += """<div class='input'><span style='color:blue;'>Input:
                    </span> {}</div>""".format(
            to_html_str(pp.show(entry[0]))
        )
        result += """<div class='input'><span style='color:green;'>
                     Human-friendly Input:</span> {}</div>""".format(
            codegen.generate_source(reverse_elab(entry[0]))
        )
        if len(entry) > 1:
            result += """<div class='output'><span style='color:red;'>
                         Output:</span> {}</div>""".format(
                to_html_str(pp.show(entry[1]))
            )
            try:
                json_text = json.dumps(
                    multi_set_val_to_json_like(entry[1]), indent=4
                )
            except Exception as e:
                json_text = "EXCEPTION OCCURRED" + str(e)
            result += """<div class='output'><span style='color:green;'>
                        Human-friendly Output:</span> {}</div>""".format(
                json_text
            )
        result += "</div>\n"
        return result

    def format_log(log, index):
        result = "<ul {} id='entry_{}'>\n".format(
            "class='entry'" if len(index) > 0 else '',
            '_'.join(map(str, index)),
        )
        for i, entry in enumerate(log):
            result += "<li>\n"
            if isinstance(entry, list):
                sub_index = index + (i,)
                result += """<a href='#/' onclick='toggle("{}")'>
                             Log {}</a>\n""".format(
                    '_'.join(map(str, sub_index)),
                    '_'.join(map(str, sub_index)),
                )
                result += format_log(entry, sub_index)
            else:
                sub_index = index + (i,)
                result += format_entry(entry, sub_index)
            result += "</li>\n"
        result += "</ul>\n"
        return result

    with open(filename, "w") as f:
        f.write("<html>\n")
        f.write("<head>\n")
        f.write("<title>Log</title>\n")
        f.write("""<meta charset="UTF-8">\n""")
        f.write("<style>\n")
        f.write(".entry { margin-left: 20px; }\n")
        f.write("</style>\n")
        f.write("</head>\n")
        f.write("<body>\n")
        f.write("<h1>Log</h1>\n")
        f.write("<button onclick='foldAll()'>Fold all</button>\n")
        f.write("<button onclick='unfoldAll()'>Unfold all</button>\n")
        f.write(format_log(logs, ()))
        f.write("<script>\n")
        f.write("function toggle(index) {\n")
        f.write("  var entry = document.getElementById('entry_' + index);\n")
        f.write(
            "  entry.style.display = entry.style.display === 'none' ?"
            " 'block' : 'none';\n"
            ""
        )
        f.write("return False;}\n")
        f.write("function foldAll() {\n")
        f.write("  var entries = document.getElementsByClassName('entry');\n")
        f.write("  for (var i = 0; i < entries.length; i++) {\n")
        f.write("    entries[i].style.display = 'none';\n")
        f.write("  }\n")
        f.write("}\n")
        f.write("function unfoldAll() {\n")
        f.write("  var entries = document.getElementsByClassName('entry');\n")
        f.write("  for (var i = 0; i < entries.length; i++) {\n")
        f.write("    entries[i].style.display = 'block';\n")
        f.write("  }\n")
        f.write("}\n")
        f.write(
            """
            function foldEntry(id) {
                var entry = document.getElementById('entry_' + id);
                entry.style.display = 'none';
                var entries = entry.querySelectorAll('.entry');
                for (var i = 0; i < entries.length; i++) {
                    entries[i].style.display = 'none';
                }
            }

            function unfoldEntry(id) {
                var entry = document.getElementById('entry_' + id);
                entry.style.display = 'block';
                var entries = entry.querySelectorAll('.entry');
                for (var i = 0; i < entries.length; i++) {
                    entries[i].style.display = 'block';
                }
            }
        """
        )
        f.write("</script>\n")
        f.write("</body>\n")
        f.write("</html>\n")


def write_logs_to_file(logs: List[Any], filepath: str):
    # the logs are structured as follows:
    # Log ::= [(Input, Output), Log_1, ..., Log_n]
    # where Log_1 ... Log_n are sub logs
    do_write_logs(logs, filepath)
