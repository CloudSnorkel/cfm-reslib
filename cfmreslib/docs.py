import os.path

import textwrap
from typing import Set

desc_id = 0


def _next_desc_id():
    global desc_id
    desc_id += 1
    return f"desc{desc_id}"


class DocWriter(object):
    def __init__(self, base_path, name):
        self.base_path = base_path
        self.name = name
        self.path = os.path.join(base_path, f"{self.name}.rst")
        assert not os.path.isfile(self.path), self.path
        self.doc = ""
        self.literals = {}
        self.toc = []

    def sub_writer(self, name=None):
        self.add_toc_item(name)
        return DocWriter(self.base_path, name)

    def add_header(self, text, underline):
        self.doc += f"{text}\n{underline * len(text)}\n\n"

    def add_anchor(self, aid):
        self.doc += f".. _{aid}:\n\n"
        return f":ref:`{aid}`"

    def get_anchor(self, aid):
        return f":ref:`{aid}`"

    def add_paragraph(self, text, indent=""):
        self.doc += textwrap.indent(text + "\n\n", indent)

    def add_literal(self, name, text):
        self.literals[name] = text

    def add_unnamed_literal(self, text, indent=""):
        lid = _next_desc_id()
        self.add_literal(lid, text)
        self.add_paragraph(f"|{lid}|", indent)

    def add_code(self, language, code):
        self.doc += f".. code-block:: {language}\n\n"
        self.add_paragraph(code, "    ")

    def add_parsed_code(self, language, code):
        self.doc += f".. parsed-literal::\n\n"
        self.add_paragraph(code, "    ")

    def add_toc_item(self, item):
        self.toc.append(item)

    def write(self):
        with open(self.path, "w") as f:
            f.write(self.doc)
            for name, text in self.literals.items():
                f.write(f".. |{name}| raw:: html\n\n")
                f.write(textwrap.indent(text, "    "))
                f.write("\n\n")
            if self.toc:
                f.write(".. toctree::\n   :hidden:\n\n")
                for t in self.toc:
                    f.write(f"   {t}\n")
                f.write("\n")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write()


def shape_args_to_doc(doc: DocWriter, resource_type: str, input_shape, replacement_attributes: Set[str]):
    _shape_args_to_doc(doc, replacement_attributes, input_shape, resource_type, set())


def _shape_args_to_doc(doc: DocWriter, replacement_attributes, shape, resource_type, history: Set[str]):
    assert shape.type_name == "structure"

    doc.add_header("Syntax", "*")

    doc.add_header("JSON", "~")
    doc.add_parsed_code("json", shape_args_to_json(shape, resource_type))
    doc.add_header("YAML", "~")
    doc.add_parsed_code("yaml", shape_args_to_yaml(shape, resource_type))

    doc.add_header("Properties", "*")

    for member_name, member_shape in shape.members.items():
        doc.add_anchor(f"member_{shape.name}_{member_name}")
        doc.add_header(member_name, "~")
        doc.add_unnamed_literal(member_shape.documentation, "  ")

        required = "Yes" if shape.required_members else "No"
        doc.add_paragraph(f"""*Required*: {required}""", "  ")

        type_name = member_shape.type_name
        if type_name == "structure":
            if member_shape.name in history:
                type_name = doc.get_anchor(f"type_{member_shape.name}")
            else:
                with doc.sub_writer(f"type_{member_shape.name}") as sub_doc:
                    type_name = sub_doc.add_anchor(f"type_{member_shape.name}")
                    sub_doc.add_header(member_shape.name, "=")
                    _shape_args_to_doc(sub_doc, [], member_shape, None, history)
                history.add(member_shape.name)
        if type_name == "list":
            if member_shape.member.type_name == "structure":
                if member_shape.member.name in history:
                    type_name = doc.get_anchor(f"type_{member_shape.member.name}")
                else:
                    with doc.sub_writer(f"type_{member_shape.member.name}") as sub_doc:
                        type_name = sub_doc.add_anchor(f"type_{member_shape.member.name}")
                        sub_doc.add_header(member_shape.member.name, "=")
                        _shape_args_to_doc(sub_doc, [], member_shape.member, None, history)
                    history.add(member_shape.member.name)
            else:
                type_name = member_shape.member.type_name
            type_name = "List of " + type_name
        doc.add_paragraph(f"""*Type*: {type_name}""", "  ")

        update_replace = "Replacement" if member_name in replacement_attributes else "No interruption"
        doc.add_paragraph(f"""*Update requires*: {update_replace}""", "  ")


def shape_args_to_json(shape, resource_type):
    if resource_type:
        prefix = f'{{\n  "Type" : "{resource_type}",\n  "Properties" : {{\n' \
            '    "ServiceToken" : {"Fn::ImportValue": "cfm-reslib"},\n'
        indent = "    "
        suffix = '\n  }\n}'
    else:
        prefix = '{\n'
        indent = "  "
        suffix = '\n}'

    members = ",\n".join(f'{indent}"{n}" : {t}' for n, t in _shape_properties(shape))

    return prefix + members + suffix


def shape_args_to_yaml(shape, resource_type):
    if resource_type:
        result = f'Type: {resource_type}\nProperties :\n  ServiceToken : !ImportValue cfm-reslib\n'
        indent = "  "
    else:
        result = ""
        indent = ""

    # TODO something not based on the string result of _shape_properties?
    for n, t in _shape_properties(shape):
        result += f"{indent}{n} :"
        if t.startswith("["):
            result += f"\n{indent}  - {t.strip('[] .,')}\n"
        elif t.startswith(":ref:"):
            result += f"\n{indent}  {t}\n"
        else:
            result += f" {t}\n"

    return result


def _shape_properties(shape):
    assert shape.type_name == "structure"

    for member_name, member_shape in shape.members.items():
        linked_name = f":ref:`member_{shape.name}_{member_name}`"
        if member_shape.type_name == "structure":
            yield linked_name, f":ref:`type_{member_shape.name}`"
        elif member_shape.type_name == "list":
            if member_shape.member.type_name == "structure":
                yield linked_name, f"[ :ref:`type_{member_shape.member.name}`, ... ]"
            else:
                yield linked_name, f"[ {member_shape.member.type_name}, ... ]"
        else:
            yield linked_name, member_shape.type_name
