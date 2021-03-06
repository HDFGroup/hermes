#ifndef HERMES_ADAPTER_ENUMERATIONS_H
#define HERMES_ADAPTER_ENUMERATIONS_H
enum AdapterMode {
  DEFAULT = 0, /* all/given files are stored on file close or flush. */
  BYPASS = 1, /* all/given files are not buffered. */
  SCRATCH = 2 /* all/given files are ignored on file close or flush. */
};
#endif  // HERMES_ADAPTER_ENUMERATIONS_H
