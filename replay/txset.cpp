#include <iostream>
#include <stdexcept>
#include "trace.h"

int
main(int argc, char** argv)
try
{
    // G53T7
    // seq 9
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
