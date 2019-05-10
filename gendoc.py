import os.path
from glob import glob

import cfmreslib.docs
import cfmreslib.resources

INDEX = """Available Custom Resources
==========================

.. toctree::
   :maxdepth: 5
   :caption: Contents:
   :glob:
   :includehidden:

   res_*
"""

base = os.path.join("docs", "resources")
os.makedirs(base, exist_ok=True)

for rst in glob(os.path.join(base, "*.rst")):
    os.unlink(rst)

with open(os.path.join(base, "index.rst"), "w") as index:
    index.write(INDEX)

for res in cfmreslib.resources.ALL_RESOURCES:
    with cfmreslib.docs.DocWriter(base, f"res_{res.NAME}") as doc:
        res.write_docs(doc)
