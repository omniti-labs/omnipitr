#!/bin/bash

export use_user="$( id -u -n )"
export work_dir="$( pwd )"

cd "$( dirname "${BASH_SOURCE[0]}" )"
export test_dir="$( pwd )"

# cd to where current script resides
cd ..
export omnipitr_dir="$( pwd )"

cd "$test_dir"

# load functions from test-lib
while read source_file
do
    source "$source_file"
done < <( find test-lib/ -type f -name '[0-9]*' | sort -t/ -k2,2 -n )

cd "$work_dir"

identify_current_pg_version

setup_output_formatting

echo "! Running test on $pg_version"

prepare_test_environment

echo "> Making master"

make_master

echo "< Master ready"

echo "> Making backup off master"

make_master_backup

echo "< Master backup done and looks ok"

echo "> Starting standalone pg from master backup"

make_standalone master 54002

echo "< Standalone pg from master backup looks ok"

echo "> Starting file-based slave out of master backup"

make_normal_slave master-slave master 54003

echo "< Slave looks ok."

echo "> Make backup off normal slave"

make_slave_backup

echo "< Backup off slave worked"

echo "> Starting standalone pg from slave backup"

make_standalone master-slave 54004

echo "< Standalone pg from slave backup looks ok"

echo "> Starting file-based slave out of slave backup"

make_normal_slave slave-slave master-slave 54005

echo "< Slave looks ok."

echo "< Try to promote normal slave"

test_promotion master-slave 54003

echo "> Try to promote normal slave"

echo "< Try to promote slave off slave"

test_promotion slave-slave 54005

echo "> Try to promote slave off slave"

if (( pg_major_version >= 9 ))
then

    echo "Running on Pg 9.0 (or later), testing streaming replication"

    echo "> Starting SR-based slave out of master backup"

    make_sr_slave master-sr-slave master 54006

    echo "< Slave looks ok."

    echo "> Make backup off SR slave"

    make_sr_slave_backup

    echo "< Backup off SR worked"

    echo "> Starting standalone pg from sr slave backup"

    make_standalone master-sr-slave 54007

    echo "< Standalone pg from sr slave backup looks ok"

    echo "> Starting file-based slave out of sr slave backup"

    make_normal_slave slave-of-sr-slave master-sr-slave 54008

    echo "< Slave looks ok."

    echo "> Starting SR-based slave out of slave backup"

    make_sr_slave sr-slave-from-slave master-slave 54009

    echo "< Slave looks ok."

    echo "> Starting SR-based slave out of SR slave backup"

    make_sr_slave sr-slave-from-sr-slave master-sr-slave 54010

    echo "< Slave looks ok."

    echo "< Try to promote normal sr slave"

    test_promotion master-sr-slave 54006

    echo "> Try to promote normal sr slave"

    echo "< Try to promote sr slave off slave"

    test_promotion sr-slave-from-slave 54009

    echo "> Try to promote sr slave off slave"

    echo "< Try to promote sr slave off sr slave"

    test_promotion sr-slave-from-sr-slave 54010

    echo "> Try to promote sr slave off sr slave"
fi

echo "All done."
