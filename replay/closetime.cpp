#include "print.h"

#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

int
main(int argc, char** argv)
try
{
    // G12T14
    // seq 7
    fs::path const here = fs::path(__FILE__).parent_path();
    auto trace = replay_trace::loadTrace(here / "G12T14_trace.json", 7);
    std::cout << trace << std::endl;
}
catch (std::exception const& e)
{
    std::cerr << "ERROR: " << e.what() << std::endl;
    return 1;
}
catch (...)
{
    std::cerr << "ERROR: unknown exception" << std::endl;
    return 1;
}
