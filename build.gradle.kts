import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	id("org.springframework.boot") version "2.3.3.RELEASE"
	id("io.spring.dependency-management") version "1.0.10.RELEASE"
	kotlin("jvm") version "1.3.72"
	kotlin("plugin.spring") version "1.3.72"
	application
}

group = "uk.gov.dwp.dataworks.reconciliation"
version = "0.0.1"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
	mavenCentral()
	jcenter()
	maven(url = "https://jitpack.io")
}

dependencies {
	annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
	implementation("com.amazonaws", "aws-java-sdk-secretsmanager", "1.11.819")
	implementation("mysql:mysql-connector-java")
	implementation("org.apache.hbase:hbase-client:1.4.13")
	implementation("commons-codec:commons-codec:1.14")
	implementation("com.github.dwp:dataworks-common-logging:0.0.5")
	implementation("org.apache.commons", "commons-text", "1.8")
	testImplementation("com.nhaarman.mockitokotlin2", "mockito-kotlin", "2.2.0")
	testImplementation("org.springframework.boot:spring-boot-starter-test") {
		exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
	}
	testImplementation("io.kotlintest:kotlintest-runner-junit5:3.3.2")
}

configurations.all {
	exclude(group = "org.slf4j", module = "slf4j-log4j12")
}

tasks.withType<Test> {
	useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "1.8"
	}
}

sourceSets {
	create("integration") {
		java.srcDir(file("src/integration/kotlin"))
		compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
		runtimeClasspath += output + compileClasspath
	}
	create("unit") {
		java.srcDir(file("src/test/kotlin"))
		compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
		runtimeClasspath += output + compileClasspath
	}
}

tasks.register<Test>("integration-test") {
	description = "Runs the integration tests"
	group = "verification"
	testClassesDirs = sourceSets["integration"].output.classesDirs
	classpath = sourceSets["integration"].runtimeClasspath
	filter {
		includeTestsMatching("ReconciliationIntegrationTest*")
	}

	environment("METADATASTORE_TABLE", "ucfs")

	useJUnitPlatform { }
	testLogging {
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
		events = setOf(org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED, org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED, org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED, org.gradle.api.tasks.testing.logging.TestLogEvent.STANDARD_OUT)
	}
}

tasks.register<Test>("unit") {
	description = "Runs the unit tests"
	group = "verification"
	testClassesDirs = sourceSets["unit"].output.classesDirs
	classpath = sourceSets["unit"].runtimeClasspath

	testLogging {
		outputs.upToDateWhen {false}
		showStandardStreams = true
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
		events = setOf(org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED, org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED, org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED)
	}
}
