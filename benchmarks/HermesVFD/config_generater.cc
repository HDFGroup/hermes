#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <getopt.h>
#include <string.h>

using std::vector;
using std::cout;
using std::string;
using std::endl;
using std::ios;
using std::fstream;
using std::streamoff;
using std::stringstream;

const int NDEVICES = 4;

void PrintUsage(char *program) {
  fprintf(stderr, "Usage %s [-r]\n", program);
  fprintf(stderr, "  -b <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify block_sizes_kb entry.\n");
  fprintf(stderr, "  -c <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify capacities_mb entry.\n");
  fprintf(stderr, "  -h\n");
  fprintf(stderr, "     Print help message and exit.\n");
  fprintf(stderr, "  -m <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify mount_points entry.\n");
  fprintf(stderr, "  -n <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify num_slabs entry.\n");
  fprintf(stderr, "  -p <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify desired_slab_percentages entry.\n");
  fprintf(stderr, "  -r\n");
  fprintf(stderr, "     Modify default_placement_policy entry.\n");
  fprintf(stderr, "  -s <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify slab_unit_sizes entry.\n");
  fprintf(stderr, "  -w <num (single space dilemma)>\n");
  fprintf(stderr, "     Modify swap_mount entry.\n");
}

void FindEntry(string config_file, string entry, char *optarg,
               vector<int> *ret) {
  fstream examplefile(config_file, ios::in | ios::out);
  if (examplefile.is_open()) {
    string line;
    while (getline(examplefile, line)) {
      size_t pos = line.find(entry);
      if (pos != string::npos) {
        string args;
        args.assign(optarg);
        stringstream ss(args);

        size_t line_size = line.size();
        pos = line.find("{");
        line.erase(pos+1);
        for (int i = 0; i < NDEVICES; ++i) {
          string item;
          ss >> item;
          line.append(item);
          if (i < NDEVICES-1)
            line.append(", ");
          else
            line.append("};");

          if (ret != NULL)
            ret->push_back(stoi(item));
        }
        if (line_size > line.size()) {
          line.append(line_size-line.size(), ' ');
        } else {
          line.append("\n");
        }
        streamoff off = examplefile.tellg();
        examplefile.seekp(off-line_size-1);
        examplefile << line;
        break;
      }
    }
    examplefile.close();
  }
}

void FindEntryMulti(string config_file, string entry, char *optarg,
                    vector<int> slabs) {
  fstream examplefile(config_file, ios::in | ios::out);
  if (examplefile.is_open()) {
    string line;
    while (getline(examplefile, line)) {
      size_t pos = line.find(entry);
      if (pos != string::npos) {
        for (int i = 0; i < NDEVICES; ++i) {
          streamoff pre_off = examplefile.tellg();
          getline(examplefile, line);

          string args;
          args.assign(optarg);
          stringstream ss(args);

          size_t line_size = line.size();
          pos = line.find("{");
          line.erase(pos+1);

          string new_para;
          for (int j = 0; j < slabs[i]; ++j) {
            string item;
            ss >> item;
            line.append(item);
            new_para.append(item);
            if (j < slabs[i]-1) {
              line.append(", ");
              new_para.append(", ");
            } else {
              line.append("},");
            }
          }
          if (line_size > line.size()) {
            line.append(line_size-line.size(), ' ');
          }
          examplefile.seekp(pre_off);
          examplefile << line;
          examplefile.seekp(pre_off+line.size()+1);
        }
      }
    }
    examplefile.close();
  }
}

void ReplaceSingle(string config_file, string entry, char *optarg) {
  fstream examplefile(config_file, ios::in | ios::out);
  if (examplefile.is_open()) {
    string line;
    while (getline(examplefile, line)) {
      size_t pos = line.find(entry);
      if (pos != string::npos) {
        size_t line_size = line.size();
        pos = line.find("=");
        size_t pos_end = line.find(";");
        line.replace(pos+2, pos_end-pos-2, optarg);
        line.append(line_size-line.size(), ' ');

        streamoff off = examplefile.tellg();
        examplefile.seekp(off-line_size-1);
        examplefile << line;
        break;
      }
    }
    examplefile.close();
  }
}

int main(int argc, char **argv) {
  int opt;
  string line;
  vector<int> slabs;
  fstream myfile;

  struct option longopts[] = {
      { "help", optional_argument, NULL, 'h' },
      { "capacities_mb", required_argument, NULL, 'c' },
      { "block_sizes_kb", required_argument, NULL, 'b' },
      { "num_slabs", required_argument, NULL, 'n' },
      { "slab_unit_sizes", required_argument, NULL, 's' },
      { "desired_slab_percentages", required_argument, NULL, 'p' },
      { "mount_points", required_argument, NULL, 'm' },
      { "swap_mount", required_argument, NULL, 'w' },
      { "default_placement_policy", required_argument, NULL, 'r' },
      { "buffer_pool_arena_percentage", required_argument, NULL, 'u' },
      { "metadata_arena_percentage", required_argument, NULL, 'a' },
      { 0, 0, 0, 0 }
  };

  string config_file = "hermes.conf";
  if (argc == 2) {
    config_file = argv[1];
  }

  while (1) {
    opt = getopt_long(argc, argv, "c:b:n:s:p:m:w:r:u:a:", longopts, 0);

    if (opt == -1) {
        /* a return value of -1 indicates that there are no more options */
        break;
    }

    switch (opt) {
      case 'a': {
        ReplaceSingle(config_file, "metadata_arena_percentage", optarg);
        break;
      }
      case 'b': {
        FindEntry(config_file, "block_sizes_kb", optarg, NULL);
        break;
      }
      case 'c': {
        FindEntry(config_file, "capacities_mb", optarg, NULL);
        break;
      }
      case 'h': {
        PrintUsage(argv[0]);
        return 1;
      }
      case 'm': {
        FindEntry(config_file, "mount_points", optarg, NULL);
        break;
      }
      case 'n': {
        FindEntry(config_file, "num_slabs", optarg, &slabs);
        break;
      }
      case 'p': {
        FindEntryMulti(config_file, "desired_slab_percentages", optarg, slabs);
        break;
      }
      case 'r': {
        if (strcmp(optarg, "R") == 0 || strcmp(optarg, "r") == 0) {
          fstream examplefile(config_file, ios::in | ios::out);
          if (examplefile.is_open()) {
            string line;
            while (getline(examplefile, line)) {
              size_t pos = line.find("default_placement_policy");
              if (pos != string::npos) {
                size_t line_size = line.size();
                pos = line.find("\"");
                line.replace(pos+1, sizeof("RoundRobin"), "Random");
                line.append(line_size-line.size(), ' ');

                streamoff off = examplefile.tellg();
                examplefile.seekp(off-line_size-1);
                examplefile << line;
                break;
              }
            }
            examplefile.close();
            break;
          }
        }
        break;
      }
      case 's': {
        FindEntryMulti(config_file, "slab_unit_sizes", optarg, slabs);
        break;
      }
      case 'u': {
        ReplaceSingle(config_file, "buffer_pool_arena_percentage", optarg);
        break;
      }
      case 'w': {
        ReplaceSingle(config_file, "swap_mount", optarg);
        break;
      }
      default: {
        PrintUsage(argv[0]);
        return 1;
      }
    }
  }
  return 0;
}

