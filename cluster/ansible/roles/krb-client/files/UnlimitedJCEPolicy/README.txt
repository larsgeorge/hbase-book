
   Unlimited Strength Java(TM) Cryptography Extension Policy Files
  for the Java(TM) Platform, Standard Edition Runtime Environment 7

                               README

----------------------------------------------------------------------
CONTENTS
----------------------------------------------------------------------

     o Introduction
     o License and Terms
     o Understanding The Export/Import Issues
     o Where To Find Documentation
     o Installation
     o Questions, Support, Reporting Bugs


----------------------------------------------------------------------
Introduction
----------------------------------------------------------------------

Thank you for downloading the Unlimited Strength Java(TM) Cryptography
Extension (JCE) Policy Files for the Java(TM) Platform, Standard
Edition (Java SE) Runtime Environment 7.

Due to import control restrictions of some countries, the version of
the JCE policy files that are bundled in the Java Runtime Environment,
or JRE(TM), 7 environment allow "strong" but limited cryptography to be
used. This download bundle (the one including this README file)
provides "unlimited strength" policy files which contain no
restrictions on cryptographic strengths.

Please note that this download file does NOT contain any encryption
functionality as all such functionality is contained within Oracle's
JRE 7. This bundles assumes that the JRE 7 has already been installed.


----------------------------------------------------------------------
License and Terms
----------------------------------------------------------------------

This download bundle is part of the Java SE Platform products and is
governed by same License and Terms notices. These notices can be found
on the Java SE download site:

    http://www.oracle.com/technetwork/java/javase/documentation/index.html


----------------------------------------------------------------------
Understanding The Export/Import Issues
----------------------------------------------------------------------

JCE for Java SE 7 has been through the U.S. export review process.  The
JCE framework, along with the various JCE providers that come standard
with it (SunJCE, SunEC, SunPKCS11, SunMSCAPI, etc), is exportable.

The JCE architecture allows flexible cryptographic strength to be
configured via jurisdiction policy files. Due to the import
restrictions of some countries, the jurisdiction policy files
distributed with the Java SE 7 software have built-in restrictions on
available cryptographic strength. The jurisdiction policy files in this
download bundle (the bundle including this README file) contain no
restrictions on cryptographic strengths.  This is appropriate for most
countries. Framework vendors can create download bundles that include
jurisdiction policy files that specify cryptographic restrictions
appropriate for countries whose governments mandate restrictions. Users
in those countries can download an appropriate bundle, and the JCE
framework will enforce the specified restrictions.

You are advised to consult your export/import control counsel or
attorney to determine the exact requirements.


----------------------------------------------------------------------
Where To Find Documentation
----------------------------------------------------------------------

The following documents may be of interest to you:

    o  The Java(TM) Cryptography Architecture (JCA) Reference Guide at:

       http://download.oracle.com/javase/7/docs/technotes/guides/security

    o  The Java SE Security web site has more information about JCE,
       plus additional information about the Java SE Security Model.
       Please see:

       http://www.oracle.com/technetwork/java/javase/tech/index-jsp-136007.html


----------------------------------------------------------------------
Installation
----------------------------------------------------------------------

Notes:

  o Unix (Solaris/Linux) and Windows use different pathname separators,
    so please use the appropriate one ("\", "/") for your environment.

  o <java-home> (below) refers to the directory where the JRE was
    installed. It is determined based on whether you are running JCE
    on a JRE or a JRE contained within the Java Development Kit, or
    JDK(TM). The JDK contains the JRE, but at a different level in the
    file hierarchy. For example, if the JDK is installed in
    /home/user1/jdk1.7.0 on Unix or in C:\jdk1.7.0 on Windows, then
    <java-home> is:

        /home/user1/jdk1.7.0/jre           [Unix]
        C:\jdk1.7.0\jre                    [Windows]

    If on the other hand the JRE is installed in /home/user1/jre1.7.0
    on Unix or in C:\jre1.7.0 on Windows, and the JDK is not
    installed, then <java-home> is:

        /home/user1/jre1.7.0               [Unix]
        C:\jre1.7.0                        [Windows]

  o On Windows, for each JDK installation, there may be additional
    JREs installed under the "Program Files" directory. Please make
    sure that you install the unlimited strength policy JAR files
    for all JREs that you plan to use.


Here are the installation instructions:

1)  Download the unlimited strength JCE policy files.

2)  Uncompress and extract the downloaded file.

    This will create a subdirectory called jce.
    This directory contains the following files:

        README.txt                   This file
        local_policy.jar             Unlimited strength local policy file
        US_export_policy.jar         Unlimited strength US export policy file

3)  Install the unlimited strength policy JAR files.

    In case you later decide to revert to the original "strong" but
    limited policy versions, first make a copy of the original JCE
    policy files (US_export_policy.jar and local_policy.jar). Then
    replace the strong policy files with the unlimited strength
    versions extracted in the previous step.

    The standard place for JCE jurisdiction policy JAR files is:

        <java-home>/lib/security           [Unix]
        <java-home>\lib\security           [Windows]


-----------------------------------------------------------------------
Questions, Support, Reporting Bugs
-----------------------------------------------------------------------

Questions
---------

For miscellaneous questions about JCE usage and deployment, we
encourage you to read:

    o Information on the Java SE Security web site

      http://www.oracle.com/technetwork/java/javase/tech/index-jsp-136007.html

    o The Oracle Online Community Forums, specifically the Java
      Cryptography forum. The forums allow you to tap into the
      experience of other users, ask questions, or offer tips to others
      on a variety of Java-related topics, including JCE. There is no
      fee to participate.

      http://forums.oracle.com/
      http://forums.oracle.com/forums/forum.jspa?forumID=964  (JCE
      forum)


Support
-------

For more extensive JCE questions or deployment issues, please contact
our Technical Support staff at:

    http://support.oracle.com


Reporting Bugs
--------------

To report bugs (with sample code) or request a feature, please see:

    http://bugreport.sun.com/bugreport/

Bug reports with specific, reproducible test cases are greatly
appreciated!
