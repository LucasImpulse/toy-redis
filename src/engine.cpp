#include "engine.hpp"
#include <iostream>
#include <stdexcept> // error gaming

// helper byte rw functions

void write_int(std::ofstream& out, int value) {
	out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

int read_int(std::ifstream& in) {
	int value = 0;
	in.read(reinterpret_cast<char*>(&value), sizeof(value));
	return value;
}

Database::Database(const std::string& filename) : db_path(filename) {
	load_from_file();

	db_file.open(db_path, std::ios::app | std::ios::binary);
	if (!db_file.is_open()) {
		throw std::runtime_error("Cannot open database.");
	}
}

Database::~Database() {
	if (db_file.is_open()) {
		db_file.close();
	}
}

void Database::set(const std::string& key, const std::string& value) {
	store[key] = value;

	int key_len = key.size();
	write_int(db_file, key_len);

	db_file.write(key.c_str(), key_len);

	int val_len = value.size();
	write_int(db_file, val_len);

	db_file.write(value.c_str(), val_len);
	db_file.flush();
}

std::string Database::get(const std::string& key) {
	auto it = store.find(key);
	if (it != store.end()) {
		return it->second;
	}
	throw std::runtime_error("Key not found");
}

void Database::load_from_file() {
	std::ifstream infile(db_path, std::ios::binary);
	if (!infile.is_open()) return;

	while (true) {
		if (infile.peek() == EOF) break;

		int key_len = read_int(infile);
		if (infile.fail()) break;

		std::string key(key_len, '\0');
		infile.read(&key[0], key_len);

		int val_len = read_int(infile);
		if (infile.fail()) break;

		std::string value(val_len, '\0');
		infile.read(&value[0], val_len);

		store[key] = value;
	}
}

// because we made the write ahead log we need to delete what are like early duplicates so write can stay O(1) but we reduce space
void Database::compact() {
	std::string temp_path = db_path + ".tmp";

	std::ofstream temp_file(temp_path, std::ios::binary | std::ios::trunc);
	if (!temp_file.is_open()) {
		throw std::runtime_error("Could not create temp file");
	}

	// we loaded the map already, dump it to the temp file
	for (const auto& pair : store) {
		const std::string& key = pair.first;
		const std::string& value = pair.second;

		int key_len = key.size();
		write_int(temp_file, key_len);
		temp_file.write(key.c_str(), key_len);

		int val_len = value.size();
		write_int(temp_file, val_len);
		temp_file.write(value.c_str(), val_len);
	}

	// windows will cry if we don't close it first
	temp_file.flush();
	temp_file.close();
	db_file.close();

	// atomic swap below, remove old db and rename temp to new db, same name for old and new btw
	if (std::remove(db_path.c_str()) != 0) {
		db_file.open(db_path, std::ios::app | std::ios::binary);
		throw std::runtime_error("Failed to remove old database when compacting");
	}
	if (std::rename(temp_path.c_str(), db_path.c_str()) != 0) {
		throw std::runtime_error("Failed to rename temp to new database");
	}

	db_file.open(db_path, std::ios::app | std::ios::binary);
}