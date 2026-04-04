"""
Compiler sector — public boundary module.

This is the ONLY module other sectors may import from the compiler package.
All cross-sector consumers should import from here, never from
``mt5pipe.compiler.models``, ``mt5pipe.compiler.service``, etc. directly.

Re-exports
----------
DatasetSpec : pydantic model
    Compiler input spec describing a dataset build.
LineageManifest : pydantic model
    Immutable lineage record for a built artifact.
DatasetCompiler : class
    Orchestrates the full compile pipeline.
compile_dataset_spec : function
    Module-level convenience for compiling a spec file.
"""

from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.compiler.service import DatasetCompiler, compile_dataset_spec

__all__ = [
    # Models
    "DatasetSpec",
    "LineageManifest",
    # Services
    "DatasetCompiler",
    "compile_dataset_spec",
]
