#!/bin/bash

BIN_DIR=$( perl -le 'use Cwd qw(realpath); print realpath(shift)' "$( dirname $0 )" )
LIB_DIR=$( perl -le 'use Cwd qw(realpath); print realpath(shift)' "$( dirname $0 )/../lib" )

echo "Checking:"
echo "- $BIN_DIR"
echo "- $LIB_DIR"

BIN_COUNT=0
for x in "$BIN_DIR"/omnipitr-*
do
    BIN_COUNT=$(( $BIN_COUNT + 1 ))
done

LIB_COUNT=$( find "$LIB_DIR" -name '*.pm' -print | wc -l )

echo "$BIN_COUNT programs, $LIB_COUNT libraries."

declare -a WARNINGS ERRORS
WARNINGS_COUNT=0
ERRORS_COUNT=0

# Base checks

if [[ -z $( which ssh 2> /dev/null ) ]]
then
    WARNINGS_COUNT=$(( 1 + $WARNINGS_COUNT ))
    WARNINGS[$WARNINGS_COUNT]="you don't have ssh program available"
fi
if [[ -z $( which rsync 2> /dev/null ) ]]
then
    ERRORS_COUNT=$(( 1 + $ERRORS_COUNT ))
    ERRORS[$ERRORS_COUNT]="you don't have rsync program available"
fi

if [[ -z $( which perl 2> /dev/null ) ]]
then
    ERRORS_COUNT=$(( 1 + $ERRORS_COUNT ))
    ERRORS[$ERRORS_COUNT]="you don't have Perl?!"
else
    PERLVEROK=$( perl -e 'print $] >= 5.008 ? "ok" : "nok"' )
    if [[ "$PERLVEROK" == "nok" ]]
    then
        WARNINGS_COUNT=$(( 1 + $WARNINGS_COUNT ))
        WARNINGS[$WARNINGS_COUNT]="your Perl is old (we support only 5.8 or newer. OmniPITR might work, but was not tested on your version of Perl)"
    fi
fi

# perl code checks

for x in "$BIN_DIR"/omnipitr-* $( find "$LIB_DIR" -name '*.pm' -print )
do
    if ! perl -I"$LIB_DIR" -wc "$x" &>/dev/null
    then
        ERRORS_COUNT=$(( 1 + $ERRORS_COUNT ))
        ERRORS[$ERRORS_COUNT]="can't compile $x ?!"
    fi
done

# modules check

for MODULE in $( egrep "^use[[:space:]]" "$BIN_DIR"/omnipitr-* $( find "$LIB_DIR" -name '*.pm' -print ) | perl -pe 's/^[^:]+:use\s+//; s/[;\s].*//' | egrep -v '^OmniPITR' | sort | uniq )
do
    if ! perl -I"$LIB_DIR" -e "use $MODULE" &>/dev/null
    then
        ERRORS_COUNT=$(( 1 + $ERRORS_COUNT ))
        ERRORS[$ERRORS_COUNT]="you don't have $MODULE Perl library (should be installed together with Perl)"
    fi
done

for MODULE in Time::HiRes
do
    if ! perl -I"$LIB_DIR" -e "use $MODULE" &>/dev/null
    then
        WARNINGS_COUNT=$(( 1 + $WARNINGS_COUNT ))
        WARNINGS[$WARNINGS_COUNT]="you don't have $MODULE Perl library - it's optional, but it could help"
    fi
done

# versions check

echo "Tar version"

tar_version_ok="$( LC_ALL=C tar --version 2>/dev/null | head -n 1 | egrep '^tar \(GNU tar\) [0-9]*\.[0-9]*$' | awk '$NF >= 1.2 {print "OK"}' )"
if [[ "$tar_version_ok" != "OK" ]]
then
    ERRORS_COUNT=$(( 1 + $ERRORS_COUNT ))
    ERRORS[$ERRORS_COUNT]="tar (in \$PATH) is either not gnu tar, or gnu tar earlier than required 1.20"
fi

# Report of status

if [[ $WARNINGS_COUNT -gt 0 ]]
then
    echo "Warnings:"
    for WARNING in "${WARNINGS[@]}"
    do
        echo "- $WARNING"
    done
fi
if [[ $ERRORS_COUNT -gt 0 ]]
then
    echo "Errors:"
    for ERROR in "${ERRORS[@]}"
    do
        echo "- $ERROR"
    done
fi
if [[ "$WARNINGS_COUNT" -eq 0 && "$ERRORS_COUNT" -eq 0 ]]
then
    echo "All checked, and looks ok."
    exit
fi
echo -n "All checked. "
if [[ "$WARNINGS_COUNT" -eq 0 ]]
then
    echo -n "No warnings. "
elif [[ "$WARNINGS_COUNT" -eq 1 ]]
then
    echo -n "1 warning. "
else
    echo -n "${WARNINGS_COUNT} warnings. "
fi
if [[ "$ERRORS_COUNT" -eq 0 ]]
then
    echo -n "No errors."
elif [[ "$ERRORS_COUNT" -eq 1 ]]
then
    echo -n "1 error."
else
    echo -n "${ERRORS_COUNT} errors."
fi

echo
