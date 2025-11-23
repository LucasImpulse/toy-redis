#include <pybind11/pybind11.h>
#include "engine.hpp"

namespace py = pybind11;

PYBIND11_MODULE(toy_redis, m) {
	m.doc() = "Custom C++ database engine";

	py::class_<Database>(m, "Database")
		.def(py::init<const std::string&>())
		.def("set", &Database::set)
		.def("get", &Database::get)
		.def("compact", &Database::compact);
}