<?php

// setup usage text
$usage = "Usage:\n";
$usage .= "  -h/--help Print this man page\n";
$usage .= "  [dir] \"[SQL]\" Where dir is the location of the CSV tables. And SQL\n";
$usage .= "  is the query to execute on them.\n";

// print man page
if (!isset($argv[1]) || '' == $argv[1] || '-h' == $argv[1] || '--help' == $argv[1]) {
    echo "\n";
    echo "SQL Parser\n";
    echo "==========\n";
    echo "\n";
    echo "A simple PHP program for SQLing CSV tables.\n";
    echo "\n$usage\n";
    die;
}

// print usage if missing 2nd arg
if (isset($argv[1]) && !isset($argv[2])) {
    echo "\n$usage\n";
    die;
}

// pull out args
$tablePath = $argv[1];
$query = $argv[2];

// check if the path to the tables is valid
if (!file_exists($tablePath)) {
    echo "\nFile path is invalid\n";
    echo "\n$usage\n";
    die;
}

// explode query statement
$pieces = explode(' ', trim($query,'" '));
//print_r($pieces);

// only SELECT allowed
if ('select' != strtolower($pieces[0])) {
    echo "\nYour query: $query\n";
    echo "However, currently only simple SELECT queries are supported.\n";
    echo "\n$usage\n";
    die;
}

// missing column, from or table
if (!isset($pieces[1]) || !isset($pieces[2]) || !isset($pieces[3])) {
    echo "\nYour query: $query\n";
    echo "However, this is not a valid query.\n";
    echo "\n$usage\n";
    die;
}

// grab selected columns -- need to make functional
$i = 1;
$columns = [];
while ('from' != strtolower($pieces[$i])) {
    $columns[] = trim($pieces[$i], ',');
    $i++;
}
//print_r($columns);

$i++;
$table = $pieces[$i];

// see if table exists
if (!file_exists($tablePath.'/'.$table.'.csv')) {
    echo "\nYour query: $query\n";
    echo "However, the table $table doesn't exist (no file $tablePath/$table.csv).\n";
    echo "\n$usage\n";
    die;
}

// parse the csv table into an array -- need to make functional
$tableFile = fopen($tablePath.'/'.$table.'.csv', 'r');
$tableColumns = fgetcsv($tableFile);
//print_r($tableColumns);
$missingColumns = '';
foreach ($columns as $column) {
    if (!in_array($column, $tableColumns)) {
        $missingColumns .= "$column is not a valid (case sensitive) column name.\n";
    }
}
if ('' != $missingColumns) {
    echo "\nYour query: $query\n";
    echo $missingColumns;
    echo "\n$usage\n";
    die;
}
$tableArray = [];
$j = 1;
while ($row = fgetcsv($tableFile)) {
    //print_r($row);
    $k = 0;
    foreach ($tableColumns as $columnName) {
        $tableArray[$j][$columnName] = $row[$k];
        $k++;
    }
    $j++;
}
//print_r($tableArray);

// parse & apply WHERE clause
$i++;
if (isset($pieces[$i])) {
    // WHERE clause exists?
    if ('where' != strtolower($pieces[$i])) {
        echo "\nYour query: $query\n";
        echo "However, currently only simple SELECT queries with WHERE clauses\n";
        echo "are supported.\n";
        echo "\n$usage\n";
        die;
    }

    $i++;
    $operand1 = $pieces[$i];

    $i++;
    $comparison = $pieces[$i];

    // is comparison valid?
    //$comparisons = ['=', '!=', '>', '<', '>=', '<=', '<>'];
    $comparisons = ['='];
    if (!in_array($comparison, $comparisons)) {
        echo "\nYour query: $query\n";
        echo "However, you used the unsupported comparison '$comparison'. Valid\n";
        echo "supported comparisons include ".implode($comparisons, ', ').".\n";
        echo "\n$usage\n";
        die;
    }

    $i++;
    $operand2 = $pieces[$i];

    // no more clauses yet
    $i++;
    if (isset($pieces[$i])) {
        echo "\nYour query: $query\n";
        echo "However, currently only simple SELECT queries with WHERE clauses\n";
        echo "are supported (no AND, OR, etc).\n";
        echo "\n$usage\n";
        die;
    }

    // apply WHERE clause
    if (in_array($operand1, $tableColumns) && in_array($operand2, $tableColumns)) {
        foreach ($tableArray as $index =>$row) {
            if ($row[$operand1] != $row[$operand2]) {
                unset($tableArray[$index]);
            }
        }
    } else if (in_array($operand1, $tableColumns) && !in_array($operand2, $tableColumns)) {
        foreach ($tableArray as $index =>$row) {
            if ($row[$operand1] != $operand2) {
                unset($tableArray[$index]);
            }
        }
    } else if (!in_array($operand1, $tableColumns) && in_array($operand2, $tableColumns)) {
        foreach ($tableArray as $index =>$row) {
            if ($operand1 != $row[$operand2]) {
                unset($tableArray[$index]);
            }
        }
    } else { // neither operand is a table column
        if ($operand1 != $operand2) {
            $tableArray = [];
        }
    }
}

// SELECT only the column(s) of interest
foreach ($tableArray as &$row) {
    foreach ($row as $index => &$value) {
        if (!in_array($index, $columns)) {
            unset($row[$index]);
        }
    }
}
//print_r($tableArray);

// print results -- need to make functional
print_r($columns);
$toprow = implode(' | ', $columns);
echo "\n| $toprow |\n";
foreach ($tableArray as $row) {
    echo "| ";
    foreach ($columns as $name) {
        echo $row[$name].' | ';
    }
    echo "\n";
}
