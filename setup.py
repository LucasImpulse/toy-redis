from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext

ext_modules = [
    Pybind11Extension(
        "flashkv",
        ["src/main.cpp", "src/engine.cpp"],
        cxx_std=17,
    ),
]

setup(
    name="flashkv",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)