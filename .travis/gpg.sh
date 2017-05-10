#!/usr/bin/env bash

set -ev

# create a random passphrase
export GPG_PASSPHRASE=$(echo "$RANDOM$(date)" | md5sum | cut -d\  -f1)

# configuration to generate gpg keys
cat >gen-key-script <<EOF
      %echo Generating a basic OpenPGP key
      Key-Type: RSA
      Key-Length: 4096
      Subkey-Type: 1
      Subkey-Length: 4096
      Name-Real: Olastic
      Name-Email: tarasrng@ukr.net
      Expire-Date: 2y
      Passphrase: ${GPG_PASSPHRASE}
      %commit
      %echo done
EOF

# create a local keypair with given configuration
gpg --batch --gen-key gen-key-script


# export created GPG key

export GPG_KEYNAME=$(gpg -K | grep ^sec | cut -d/  -f2 | cut -d\  -f1 | head -n1)

# cleanup local configuration
shred gen-key-script

# publish the gpg key  (sonatype reads from keyserver.ubuntu.com)
gpg --keyserver keyserver.ubuntu.com --send-keys ${GPG_KEYNAME}

# wait for the key beeing accessible
while(true); do
    date
    gpg --keyserver keyserver.ubuntu.com --recv-keys ${GPG_KEYNAME} && break || sleep 30
done