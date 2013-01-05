#!perl -T

use Test::More tests => 30;

BEGIN {
    use_ok( 'OmniPITR::Log' )                                           || print "Bail out on OmniPITR::Log\n";
    use_ok( 'OmniPITR::Pidfile' )                                       || print "Bail out on OmniPITR::Pidfile\n";
    use_ok( 'OmniPITR::Program' )                                       || print "Bail out on OmniPITR::Program\n";
    use_ok( 'OmniPITR::Program::Archive' )                              || print "Bail out on OmniPITR::Program::Archive\n";
    use_ok( 'OmniPITR::Program::Backup' )                               || print "Bail out on OmniPITR::Program::Backup\n";
    use_ok( 'OmniPITR::Program::Backup::Master' )                       || print "Bail out on OmniPITR::Program::Backup::Master\n";
    use_ok( 'OmniPITR::Program::Backup::Slave' )                        || print "Bail out on OmniPITR::Program::Backup::Slave\n";
    use_ok( 'OmniPITR::Program::Cleanup' )                              || print "Bail out on OmniPITR::Program::Cleanup\n";
    use_ok( 'OmniPITR::Program::Monitor' )                              || print "Bail out on OmniPITR::Program::Monitor\n";
    use_ok( 'OmniPITR::Program::Monitor::Check' )                       || print "Bail out on OmniPITR::Program::Monitor::Check\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Archive_Queue' )        || print "Bail out on OmniPITR::Program::Monitor::Check::Archive_Queue\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Current_Archive_Time' ) || print "Bail out on OmniPITR::Program::Monitor::Check::Current_Archive_Time\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Current_Restore_Time' ) || print "Bail out on OmniPITR::Program::Monitor::Check::Current_Restore_Time\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Dump_State' )           || print "Bail out on OmniPITR::Program::Monitor::Check::Dump_State\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Errors' )               || print "Bail out on OmniPITR::Program::Monitor::Check::Errors\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Last_Archive_Age' )     || print "Bail out on OmniPITR::Program::Monitor::Check::Last_Archive_Age\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Last_Backup_Age' )      || print "Bail out on OmniPITR::Program::Monitor::Check::Last_Backup_Age\n";
    use_ok( 'OmniPITR::Program::Monitor::Check::Last_Restore_Age' )     || print "Bail out on OmniPITR::Program::Monitor::Check::Last_Restore_Age\n";
    use_ok( 'OmniPITR::Program::Monitor::Parser' )                      || print "Bail out on OmniPITR::Program::Monitor::Parser\n";
    use_ok( 'OmniPITR::Program::Monitor::Parser::Archive' )             || print "Bail out on OmniPITR::Program::Monitor::Parser::Archive\n";
    use_ok( 'OmniPITR::Program::Monitor::Parser::Backup' )              || print "Bail out on OmniPITR::Program::Monitor::Parser::Backup\n";
    use_ok( 'OmniPITR::Program::Monitor::Parser::Backup_Master' )       || print "Bail out on OmniPITR::Program::Monitor::Parser::Backup_Master\n";
    use_ok( 'OmniPITR::Program::Monitor::Parser::Backup_Slave' )        || print "Bail out on OmniPITR::Program::Monitor::Parser::Backup_Slave\n";
    use_ok( 'OmniPITR::Program::Monitor::Parser::Restore' )             || print "Bail out on OmniPITR::Program::Monitor::Parser::Restore\n";
    use_ok( 'OmniPITR::Program::Restore' )                              || print "Bail out on OmniPITR::Program::Restore\n";
    use_ok( 'OmniPITR::Program::Synch' )                                || print "Bail out on OmniPITR::Program::Synch\n";
    use_ok( 'OmniPITR::Tools' )                                         || print "Bail out on OmniPITR::Tools\n";
    use_ok( 'OmniPITR::Tools::CommandPiper' )                           || print "Bail out on OmniPITR::Tools::CommandPiper\n";
    use_ok( 'OmniPITR::Tools::NetGet' )                                 || print "Bail out on OmniPITR::Tools::NetGet\n";
    use_ok( 'OmniPITR::Tools::ParallelSystem' )                         || print "Bail out on OmniPITR::Tools::ParallelSystem\n";
}

diag( "Testing Pg::SQL::Parser $Pg::SQL::Parser::VERSION, Perl $], $^X" );
