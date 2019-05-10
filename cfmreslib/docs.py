import os.path

import textwrap

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
        # self.doc += f".. code-block:: {language}\n\n"
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
