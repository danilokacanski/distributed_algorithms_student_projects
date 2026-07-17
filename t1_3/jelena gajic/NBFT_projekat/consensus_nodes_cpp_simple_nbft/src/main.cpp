#include "Network.hpp"

#include <exception>
#include <iostream>
#include <string>

void printUsage() {
    std::cout << "Upotreba:\n"
              << "  consensus_simple.exe <scenario> <ispravni> <Byzantine> <velicina_grupe>\n\n"
              << "Scenariji:\n"
              << "  normal                  - svi cvorovi su ispravni\n"
              << "  byzantine_wrong_value   - obicni Byzantine cvorovi glasaju za drugu vrednost\n"
              << "  faulty_rep_low          - predstavnik prve grupe salje premalo potpisa\n"
              << "  primary_silent          - primary ne odgovara\n\n"
              << "Primer:\n"
              << "  .\\consensus_simple.exe normal 17 0 4\n";
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        printUsage();
        return 1;
    }

    try {
        const std::string scenario = argv[1];
        const int honestNodes = std::stoi(argv[2]);
        const int byzantineNodes = std::stoi(argv[3]);
        const int groupSize = std::stoi(argv[4]);
        if (honestNodes < 0 || byzantineNodes < 0) {
            throw std::runtime_error("Broj cvorova ne moze biti negativan.");
        }

        Network network(honestNodes + byzantineNodes, groupSize);
        network.initialize();
        network.configureScenario(scenario, byzantineNodes);
        network.printConfiguration();
        network.start();

        const bool success = network.runConsensus("BLOCK_1", 2500);
        network.stop();
        network.printFinalState();

        if (success) {
            std::cout << "\nNBFT konsenzus je uspesno zavrsen.\n";
        } else {
            std::cout << "\nNBFT konsenzus nije postignut u zadatom vremenu.\n";
        }
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "Greska: " << error.what() << '\n';
        printUsage();
        return 1;
    }
}
