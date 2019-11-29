/*
The contents of this file are subject to the terms of the
Common Development and Distribution License (the "License").
You may not use this file except in compliance with the License.

You can obtain a copy of the license at docs/cddl1.txt or
http://opensource.org/licenses/CDDL-1.0.
See the License for the specific language governing permissions
and limitations under the License.

When distributing Covered Code, include this CDDL HEADER in each
file and include the License file at docs/cddl1.txt.

Portions Copyright 2019 EdgeDB Inc.
Copyright 2016 Netflix, Inc.
Copyright 2011 Joyent, Inc.  All rights reserved.
Copyright 2011 Brendan Gregg.  All rights reserved.
*/

var details, searchbtn, svg, searching;
function init(evt) {
    details = document.getElementById("details").firstChild;
    searchbtn = document.getElementById("search");
    svg = document.getElementsByTagName("svg")[0];
    searching = 0;
}

// mouse-over for info
function s(node) {
    value = "";
    if (node !== undefined) {
        title = node.getElementsByTagName("title");
        if (title.length == 1) {
            value = "Function: " + title[0].textContent;
        }
    }
    details.nodeValue = value;
}

// ctrl-F for search
window.addEventListener("keydown",function (e) {
    if (e.keyCode === 114 || (e.ctrlKey && e.keyCode === 70)) {
        e.preventDefault();
        search_prompt();
    }
})

// functions
function find_child(parent, name, attr) {
    var children = parent.childNodes;
    for (var i=0; i<children.length;i++) {
        if (children[i].tagName == name)
            return (attr != undefined) ? children[i].attributes[attr].value : children[i];
    }
    return;
}
function orig_save(e, attr, val) {
    if (e.attributes["_orig_"+attr] != undefined) return;
    if (e.attributes[attr] == undefined) return;
    if (val == undefined) val = e.attributes[attr].value;
    e.setAttribute("_orig_"+attr, val);
}
function orig_load(e, attr) {
    if (e.attributes["_orig_"+attr] == undefined) return;
    e.attributes[attr].value = e.attributes["_orig_"+attr].value;
    e.removeAttribute("_orig_"+attr);
}
function g_to_text(e) {
    var text = find_child(e, "title").firstChild.nodeValue;
    return (text)
}
function g_to_func(e) {
    var func = g_to_text(e);
    if (func != null)
        func = func.replace(/ .*/, "");
    return (func);
}

// zoom
function zoom_reset(e) {
    if (e.attributes != undefined) {
        orig_load(e, "x");
        orig_load(e, "width");
    }
    if (e.childNodes == undefined) return;
    for(var i=0, c=e.childNodes; i<c.length; i++) {
        zoom_reset(c[i]);
    }
}
function zoom_child(e, x, ratio) {
    if(e.tagName != "svg")  {
        return;
    }
    if (e.attributes != undefined) {
        if (e.attributes["x"] != undefined) {
            orig_save(e, "x");
            e.attributes["x"].value = (parseFloat(e.attributes["x"].value) - x) * ratio;
        }
        if (e.attributes["width"] != undefined) {
            orig_save(e, "width");
            e.attributes["width"].value = parseFloat(e.attributes["width"].value) * ratio;
        }
    }

    if (e.childNodes == undefined) return;
    for(var i=0, c=e.childNodes; i<c.length; i++) {
        zoom_child(c[i], x, ratio);
    }
}
function zoom_parent(e) {
    if(e.tagName != "svg")  {
        return;
    }
    if (e.attributes) {
        if (e.attributes["x"] != undefined) {
            orig_save(e, "x");
            e.attributes["x"].value = 0;
        }
        if (e.attributes["width"] != undefined) {
            orig_save(e, "width");
            e.attributes["width"].value = parseInt(svg.width.baseVal.value);
        }
    }
    if (e.childNodes == undefined) return;
    for(var i=0, c=e.childNodes; i<c.length; i++) {
        zoom_parent(c[i]);
    }
}
function zoom(node, upsidedown) {
    var attr = node.attributes;
    var width = parseFloat(attr["width"].value);
    var xmin = parseFloat(attr["x"].value);
    var xmax = parseFloat(xmin + width);
    var ymin = parseFloat(attr["y"].value);
    var ratio = (svg.width.baseVal.value) / width;

    // XXX: Workaround for JavaScript float issues (fix me)
    var fudge = 0.0001;

    var unzoombtn = document.getElementById("unzoom");
    unzoombtn.style["opacity"] = "1.0";

    var el = document.getElementsByTagName("svg");
    for(var i=0;i<el.length;i++){
        var e = el[i];
        var a = e.attributes;
        if (a["class"].value !== "func_g")
            continue;

        var ex = parseFloat(a["x"].value);
        var ew = parseFloat(a["width"].value);

        // Is it an ancestor
        if (upsidedown === true) {
            var upstack = parseFloat(a["y"].value) < ymin;
        } else {
            var upstack = parseFloat(a["y"].value) > ymin;
        }
        if (upstack) {
            // Direct ancestor
            if (ex <= xmin && (ex+ew+fudge) >= xmax) {
                e.style["opacity"] = "0.5";
                zoom_parent(e);
                e.onclick = function(e){unzoom(); zoom(this, upsidedown);};
                //#update_text(e);
            }
            // not in current path
            else
                e.style["display"] = "none";
        }
        // Children maybe
        else {
            // no common path
            if (ex < xmin || ex + fudge >= xmax) {
                e.style["display"] = "none";
            }
            else {
                zoom_child(e, xmin, ratio);
                e.onclick = function(e){zoom(this, upsidedown);};
                //#update_text(e);
            }
        }
    }
}
function unzoom() {
    var unzoombtn = document.getElementById("unzoom");
    unzoombtn.style["opacity"] = "0.0";

    var el = document.getElementsByTagName("svg");
    for(i=0;i<el.length;i++) {
        var e = el[i];
        if (e.attributes["class"].value !== "func_g")
            continue;
        e.style["display"] = "block";
        e.style["opacity"] = "1";
        zoom_reset(e);
        //#update_text(e);
    }
}

// search
function reset_search() {
    var el = document.getElementsByTagName("rect");
    for (var i=0; i < el.length; i++){
        orig_load(el[i], "fill")
    }
}
function search_prompt() {
    if (!searching) {
        var term = prompt("Enter a search term (regexp " +
            "allowed, eg: ^compile_)", "");
        if (term != null) {
            search(term)
        }
    } else {
        reset_search();
        searching = 0;
        searchbtn.style["opacity"] = "0.1";
        searchbtn.firstChild.nodeValue = "Search"
    }
}
function search(term) {
    var re = new RegExp(term);
    var el = document.getElementsByTagName("svg");
    for (var i = 0; i < el.length; i++) {
        var e = el[i];
        if (e.attributes["class"].value != "func_g")
            continue;
        var func = g_to_func(e);
        var rect = find_child(e, "rect");
        if (func == null || rect == null)
            continue;

        if (func.match(re)) {
            // highlight
            orig_save(rect, "fill");
            rect.attributes["fill"].value = "rgb(230,0,230)";
            searching = 1;
        }
    }
    if (!searching)
        return;

    searchbtn.style["opacity"] = "1.0";
    searchbtn.firstChild.nodeValue = "Reset Search"
}
function searchover(e) {
    searchbtn.style["opacity"] = "1.0";
}
function searchout(e) {
    if (searching) {
        searchbtn.style["opacity"] = "1.0";
    } else {
        searchbtn.style["opacity"] = "0.1";
    }
}
