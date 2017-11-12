#!/bin/bash
for data in */
do
  pushd $data/archived/tweets

  for tarball in *.tar.gz
  do
    echo "unzip "$tarball"..."
    tar -xzvf $tarball
  done

  for d in */
  do
    echo "scan "$d"..."
    pushd $d
    for file in *
    do
      echo "read "$file"..."
      json2csv.py -e $file headers.json
    done
    popd
  done

  popd
done

