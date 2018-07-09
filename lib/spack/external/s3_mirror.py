##############################################################################
# Copyright (c) 2013-2018, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory.
#
# This file is part of Spack.
# Created by Todd Gamblin, tgamblin@llnl.gov, All rights reserved.
# LLNL-CODE-647188
#
# For details, see https://github.com/spack/spack
# Please also see the NOTICE and LICENSE files for our notice and the LGPL.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License (as
# published by the Free Software Foundation) version 2.1, February 1999.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the terms and
# conditions of the GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
##############################################################################

""""
A script for creating a mirror on a S3 bucket.

Given a local path, a bucket name, and optionally an AWS profile S3Mirror will recursively scan a directory structure,
uploading its contents to a S3 bucket.

AWS Credentials (from: https://boto3.readthedocs.io/en/latest/guide/configuration.html)

Credentials can be obtained one of three ways:
1. Hardcoded (discouraged)
2. Environment Variables
  1. ACCESS_KEY (required)
  2. SECRET_KEY (required)
  3. SESSION_TOKEN (optional)
3. Shared Credentials File

S3Mirror will use a shared credentials file if the name of the profile is passed as an argument. Otherwise boto3
automatically checks for the environment variables. If a profile is not present and credentials are not present via
environment variables boto3 will throw a NoCredentialsError upon its first attempt to upload a file.
"""

import os
import sys
import boto3


class S3Mirror(object):
    def __init__(self, local_path, bucket_name, profile=None):
        self.local_path = local_path
        self.bucket_name = bucket_name
        self.profile = profile

    def mirror(self):
        client = self.get_client()
        length = len(self.local_path) + 1 # plus one to remove the directory separator
        for root, subdir, files in os.walk(self.local_path):
            for file in files:
                filename = os.path.join(root, file)
                key = os.path.join(root[length:], file)
                client.upload_file(filename, self.bucket_name, key)

    def get_client(self):
        if self.profile:
            session = boto3.Session(profile_name=self.profile)
            client = session.client('s3')
        else:
            client = boto3.client('s3')

        return client


if __name__ == '__main__':
    path = sys.argv[1]
    bucket = sys.argv[2]
    try:
        prof = sys.argv[3]
    except IndexError:
        prof = None

    s3 = S3Mirror(path, bucket, prof)
    s3.mirror()

