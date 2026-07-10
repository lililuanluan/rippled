#include <iostream>
#include <stdexcept>

int
main(int argc, char** argv)
try
{
    // G12T14
    // seq 7
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