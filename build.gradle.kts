plugins {
    `java-library`
    `maven-publish`
    id("com.diffplug.spotless") version "6.23.3"
}

group = "org.mitre"
version = "0.0.1"

repositories {
    mavenCentral()
    mavenLocal()
}



dependencies {
    implementation("com.google.guava:guava:32.1.2-jre")
    implementation("com.google.code.gson:gson:2.8.9")
    implementation("org.mitre:commons:0.0.57") //required for TimeId (not just LatLong examples)
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.6")
    implementation("org.duckdb:duckdb_jdbc:1.2.0")

    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.5.13")

    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testImplementation("org.hamcrest:hamcrest-all:1.3")
}

//  This idiom completely mutes the "javadoc" task from java-library.
//  Javadoc is still produced, but you won't get warnings OR build failures due to javadoc
//  I decided to turn warning off because the amount of javadoc required for builder was too much.
tasks {
    javadoc {
        options {
            (this as CoreJavadocOptions).addStringOption("Xdoclint:none", "-quiet")
        }
    }
}


/** Test task that only runs SLOW tests  */
tasks.register<Test>("testSlow") {
    group = "verification"
    description = "Runs tests tagged with \"SLOW\""
    useJUnitPlatform {
        includeTags("SLOW")
    }

    // The unit tests that make maps require 1G memory
    maxHeapSize = "1G"

    testLogging {
        events("PASSED", "SKIPPED", "FAILED")
    }

    failFast = true
}


/** Test task that excludes SLOW tests  */
tasks.register<Test>("testFast") {
    group = "verification"
    description = "Runs tests, but excludes test tagged with \"SLOW\""
    useJUnitPlatform {
        excludeTags("SLOW")
    }

    // The unit tests that make maps require 1G memory
    maxHeapSize = "1G"

    testLogging {
        events("PASSED", "SKIPPED", "FAILED")
    }

    failFast = true
}

// Simply run all tests.
// DO NOT use dependOn("testFast") or dependOn("testSlow") because that will run those tasks
// when you run a single unit test in your IDE, and no one wants to kick off stress tests when
// rerunning individual unit tests.
tasks.named<Test>("test") {
    group = "verification"
    description = "Runs all test, tests tagged with \"SLOW\" are run last."

    // This needs to be here even if testFast & testSlow cover everything
    useJUnitPlatform {
    }

    // The unit tests that make maps require 1G memory
    maxHeapSize = "1G"

    testLogging {
        events("PASSED", "SKIPPED", "FAILED")
    }

    failFast = true
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
    withSourcesJar()
    withJavadocJar()
}

// Enforces a consistent code style with the "spotless" plugin
//
// See https://github.com/diffplug/spotless
// And https://github.com/diffplug/spotless/tree/main/plugin-gradle
spotless {
    java {

        // This formatter is "Good" but not "Perfect"
        // But, using this formatter allows multiple contributors to work in a "common style"
        palantirJavaFormat()

        // import order: static, JDK, MITRE, 3rd Party
        importOrder("\\#", "java|javax", "org.mitre", "")
        removeUnusedImports()

        // indentWithTabs(4)
        // indentWithSpaces(2)
    }
}


publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "dist-tree"
            from(components["java"])
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set(project.name)
                description.set("Metric & B Tree in one")
                url.set("https://github.com/mitre-public/dist-tree")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("jparker")
                        name.set("Jon Parker")
                        email.set("jiparker@mitre.org")
                    }
                }

                //REQUIRED! To publish to maven central (from experience the leading "scm:git" is required too)
                scm {
                    connection.set("scm:git:https://github.com/mitre-public/dist-tree.git")
                    developerConnection.set("scm:git:ssh://git@github.com:mitre-public/dist-tree.git")
                    url.set("https://github.com/mitre-public/dist-tree")
                }
            }
        }
    }
}