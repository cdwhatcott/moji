#!/bin/bash
TRACKERS=${moji.tracker.hosts}
DOMAIN=${moji.domain}
KEY_PREFIX=${test.moji.key.prefix}
CLASS=${test.moji.class.a}
IFS=$'\n';

# For durable write test
function disable_replicate {
  echo -e "\n\n\033[32m[WARN] The test will disable replicate worker. Please enable it manually after test.\033[0m\n\n"
  host=$(echo $TRACKERS | cut -f1 -d:)
  port=$(echo $TRACKERS | cut -f2 -d:)
  # Use netcat-openbsd. May not support old versions: 1.89-3ubuntu2 is not ok; 1.105-7ubuntu1 is ok.
  echo '!want 0 replicate' | nc $host $port
}


function clear_test_data {
  KEYS=`moglistkeys $KEY_PREFIX --trackers=$TRACKERS --domain=$DOMAIN`

  for key in $KEYS
  do
    if [[ $key == *" files found"* ]]
    then
      break;
    fi
    echo "Deleting: '$key' ..."
    mogdelete $key --trackers=$TRACKERS --domain=$DOMAIN
  done;
}

function upload_new_random_file {
  head /dev/urandom | uuencode -m - | mogupload --trackers=$TRACKERS --domain=$DOMAIN --key="$KEY_PREFIX$1" --class=$CLASS --file="-"
  echo "Created mogile file: '$KEY_PREFIX$1'"
}
  
function upload_file {
  mogupload --trackers=$TRACKERS --domain=$DOMAIN --key="$KEY_PREFIX$1" --class=$CLASS --file="$2"
  echo "Created mogile file: '$KEY_PREFIX$1'"
}

if [ -z "$KEY_PREFIX" ]; then
    echo "You MUST declare a key prefix"
    exit 1;
fi

clear_test_data
upload_new_random_file overwriteThenReadBack
upload_new_random_file exists
upload_new_random_file notExistsAfterDelete
upload_new_random_file rename
upload_new_random_file renameExistingKey1
upload_new_random_file renameExistingKey2
upload_new_random_file updateStorageClass
upload_new_random_file updateStorageClassToUnknown
upload_new_random_file list1
upload_new_random_file list2
upload_new_random_file list3
upload_new_random_file getPaths

upload_file fileOfKnownSize data/fileOfKnownSize.dat
upload_file attributes data/fileOfKnownSize.dat
upload_file mogileFileCopyToFile data/mogileFileCopyToFile.dat

disable_replicate
exit 0
