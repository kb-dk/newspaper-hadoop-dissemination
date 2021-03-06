1.6
* Introduce check to ensure that generated dissemination copies are not of size 0.
* Fix classpath problems from earlier releases

1.5
* Use newest version of item event framework. No functional changes for this module.
* Do not depend on MFPAK events to trigger this component.
* Configuration has been extended and changed and example config has been updated. Please update your configuration files.

1.4
* Added a reduce step which creates symlinks to all the dissemination files

This change introduces two new config parameters:

    # Root directory where symlinks are created
    symlink.rootdir.path=/avis-show/symlinks
    # Depth of directory-nesting for symlinks
    symlink.depth=4

* Added a command line utility for creating symlinks per batch roundtrip for batches which have already been processed.
See the script createSymlinks.sh .


1.3
* Update to batch event framework 1.9 and mfpak integration 1.9 to fix the 'does not find any work bug'

1.2
* Disabled speculative execution
* Use MFPAK triggers
* Replaced sample hadoop configs with those for the cloudera cluster
* Updated to newspaper-parent 1.2
* Changed hadoop dependencies to Cloudera based 2.0.0-cdh4.5.0 
* Update framework dependency to 1.7

1.1.1 
* Updated framework dependency to 1.4.5 to enable maxResults

1.1
* Update to newspaper-batch-event-framework 1.4.2
* Remove System.out.println() calls in non-test code

1.0
Initial release
 - Works as a autonomous component
 - Programmatic job submission to the cluster
