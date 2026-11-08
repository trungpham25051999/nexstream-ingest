#pragma once
#include <iostream>
#include <string_view>
class TestHarness {
  public:
    void expect(bool value, std::string_view message) {
        if (!value) { ++failures_; std::cerr << "FAIL: " << message << '\n'; }
    }
    int finish() const {
        if (failures_ == 0) std::cout << "PASS: all assertions\n";
        return failures_ == 0 ? 0 : 1;
    }
  private:
    int failures_{0};
};
