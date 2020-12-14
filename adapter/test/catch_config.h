#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>
using namespace Catch::clara;

Parser define_options();

int init();
int finalize();

int main( int argc, char* argv[] ){
    Catch::Session session; // There must be exactly one instance
    // Build a new parser on top of Catch's
    auto cli = session.cli() // Get Catch's composite command line parser
               |  define_options();        // description string for the help output
    int returnCode =init();
    if( returnCode != 0 ) // Indicates a command line error
        return returnCode;
    // Now pass the new composite back to Catch so it uses that
    session.cli(cli);

    // Let Catch (using Clara) parse the command line
    returnCode = session.applyCommandLine( argc, argv );
    if( returnCode != 0 ) // Indicates a command line error
        return returnCode;
    int test_return_code = session.run();
    returnCode = finalize();
    if( returnCode != 0 ) // Indicates a command line error
        return returnCode;
    return test_return_code;
}