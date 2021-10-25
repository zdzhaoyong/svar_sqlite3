cmake_minimum_required(VERSION 2.8)
project(svar_sqlite3)

include(PICMake)
set(CMAKE_CXX_STANDARD 11)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()


pi_add_target(svar_sqlite3 SHARED src REQUIRED System)

pi_report_target()

# Now do make install
pi_install(TARGETS svar_sqlite3)
