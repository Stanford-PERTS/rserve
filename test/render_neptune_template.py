#!/usr/bin/env python2.7

# Testing EP reports using the jinja-adapted template.

import jinja2
import json
import optparse
import os
import sys


subdirs = [
    ('neptune/app',),  # /app, python server code
    # include subdirectories, e.g. dir1/dir2, like this:
    #('dir1', 'dir2')
]

for path_parts in subdirs:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), *path_parts))

import copilot_report_config

def render_template(template, **template_data):
    dirname, filename = os.path.split(os.path.abspath(template))
    jinja_environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(dirname),
        extensions=['jinja2.ext.autoescape'],
        autoescape=True
    )

    return jinja_environment.get_template(filename).render(**template_data)

def write_html(template, data, out):
    with open(data, 'r') as file_handle:

        json_str = file_handle.read()
        template_data = json.loads(json_str)

    kwargs = dict(template_data, config=copilot_report_config.config)
    html_string = render_template(template, **kwargs)

    with open(out, 'w') as file_handle:
        file_handle.write(html_string.encode("utf-8"))

parser = optparse.OptionParser()
parser.add_option("-t", "--template", action="store", dest="template",
                  help="Jinja/html template file path.")
parser.add_option("-d", "--data", action="store", dest="data",
                  help="JSON data file path.")
parser.add_option("-o", "--out", action="store", dest="out",
                  help="Output (rendered report html) file path.")
(options, args) = parser.parse_args()

write_html(**options.__dict__)
