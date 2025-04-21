<a href="https://opensource.newrelic.com/oss-category/#archived"><picture><source media="(prefers-color-scheme: dark)" srcset="https://github.com/newrelic/opensource-website/raw/main/src/images/categories/dark/Archived.png"><source media="(prefers-color-scheme: light)" srcset="https://github.com/newrelic/opensource-website/raw/main/src/images/categories/Archived.png"><img alt="New Relic Open Source archived project banner." src="https://github.com/newrelic/opensource-website/raw/main/src/images/categories/Archived.png"></picture></a>


![GitHub forks](https://img.shields.io/github/forks/newrelic-experimental/newrelic-java-couchbase?style=social)
![GitHub stars](https://img.shields.io/github/stars/newrelic-experimental/newrelic-java-couchbase?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/newrelic-experimental/newrelic-java-couchbase?style=social)

![GitHub all releases](https://img.shields.io/github/downloads/newrelic-experimental/newrelic-java-couchbase/total)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/newrelic-experimental/newrelic-java-couchbase)
![GitHub last commit](https://img.shields.io/github/last-commit/newrelic-experimental/newrelic-java-couchbase)
![GitHub Release Date](https://img.shields.io/github/release-date/newrelic-experimental/newrelic-java-couchbase)


![GitHub issues](https://img.shields.io/github/issues/newrelic-experimental/newrelic-java-couchbase)
![GitHub issues closed](https://img.shields.io/github/issues-closed/newrelic-experimental/newrelic-java-couchbase)
![GitHub pull requests](https://img.shields.io/github/issues-pr/newrelic-experimental/newrelic-java-couchbase)
![GitHub pull requests closed](https://img.shields.io/github/issues-pr-closed/newrelic-experimental/newrelic-java-couchbase)
# Couchbase Data Source Instrumentation

Instrumentation to track calls to Couchbase.

# Migrated to the Java Agent

Instrumentation for Couchbase Client is now included in the Java Agent for versions 8.20.0 and later.  It is recommended that you use version 8.20.0 or later of the Java Agent rather than installing these extensions.  This repository has been archived and will no longer be supported.    

## Installation

1. Download the Release jars   
2. In the New Relic Java Agent directory (the one containing newrelic.jar), create a directory named extensions   
3. Copy the downloaded jars into the extensions directory   
4. Restart the application

## Getting Started
Once the jars have been deployed, the New Relic Java Agent should start reporting calls to Couchbase under the Database tab.    


## Building

To build the instrumentation jars requires having Gradle installed.   
To Build:   
Set the environment variable NEW_RELIC_EXTENSIONS_DIR to the extensions directory of the New Relic Java Agent or a local directory   
To build a single module, run the command   
./gradlew moduleName:clean moduleName:install   
To build all modules, run the command:   
./gradlew clean install  
If NEW_RELIC_EXTENSIONS_DIR does not point to the extensions directory of the New Relic Java Agent, copy the resulting jars into the extensions directory   
Restart the application

## Support

New Relic hosts and moderates an online forum where customers can interact with New Relic employees as well as other customers to get help and share best practices. Like all official New Relic open source projects, there's a related Community topic in the New Relic Explorers Hub. You can find this project's topic/threads here:


## Contributing
We encourage your contributions to improve [project name]! Keep in mind when you submit your pull request, you'll need to sign the CLA via the click-through using CLA-Assistant. You only have to sign the CLA one time per project.
If you have any questions, or to execute our corporate CLA, required if your contribution is on behalf of a company,  please drop us an email at opensource@newrelic.com.

**A note about vulnerabilities**

As noted in our [security policy](../../security/policy), New Relic is committed to the privacy and security of our customers and their data. We believe that providing coordinated disclosure by security researchers and engaging with the security community are important means to achieve our security goals.

If you believe you have found a security vulnerability in this project or any of New Relic's products or websites, we welcome and greatly appreciate you reporting it to New Relic through [HackerOne](https://hackerone.com/newrelic).   

## License
Atomikos Data Source Instrumentation is licensed under the [Apache 2.0](http://apache.org/licenses/LICENSE-2.0.txt) License.
