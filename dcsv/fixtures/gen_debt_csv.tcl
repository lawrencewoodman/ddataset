# Generate a big csv file based around people helped to get out of debt
# This can then be used for testing/benchmarking
#
# To create the csv file just redirect the output to a file:
#   $ tclsh gen_debt_csv.tcl > debt.csv
#
# To import this into sqlite3:
#   Run sqlite3 debt.db
#   sqlite> .mode csv people
#   sqlite> .import debt.csv people
#
set numRows 10000

set fieldNames {
  name
  balance
  num_cards
  marital_status
  tertiary_educated
  success
}

set firstNames {
  Annie Franklin Bob William David Nerris Zeb Paul James Ali
  Maryam Zach Thomas Sonny Richard John Peter Clive Daniel
}

set secondNames {
  Field Ali Pixi George Robertson Clarke Hardy Olsen Hally
  Lister Harold Wesley Ousterhout Stallman Ornstein Levy
}

proc randomInt {min max} {
  return [expr {int(rand()*($max-$min+1)+$min)}]
}

proc lrandom {l} {
      lindex $l [expr {int(rand()*[llength $l])}]
}

puts [join $fieldNames ","]
for {set i 0} {$i < $numRows} {incr i} {
  set name "[lrandom $firstNames] [lrandom $secondNames]"
  set balance [randomInt -350000 -100]
  set numCards [randomInt 0 20]
  set maritalStatus [lrandom {married single divorced}]
  set tertiaryEducated [lrandom {true false}]
  set success [lrandom {true false}]
  set row [list $name $balance $numCards $maritalStatus $tertiaryEducated $success]
  puts [join $row ","]
}

