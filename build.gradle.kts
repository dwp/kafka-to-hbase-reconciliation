import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	id("org.springframework.boot") version "2.3.3.RELEASE"
	id("io.spring.dependency-management") version "1.0.10.RELEASE"
	kotlin("jvm") version "1.4.0"
	kotlin("plugin.spring") version "1.3.72"
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
	implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", "1.3.9")
	implementation("com.amazonaws:aws-java-sdk-secretsmanager:1.11.819")
	implementation("mysql:mysql-connector-java:6.0.6")
	implementation("org.apache.hbase:hbase-client:1.4.13")
	implementation("commons-codec:commons-codec:1.14")
	implementation("com.github.dwp:dataworks-common-logging:0.0.5")
	implementation("org.apache.commons:commons-text:1.8")
	implementation("org.apache.commons:commons-dbcp2:2.8.0")

	testImplementation("io.kotest:kotest-runner-junit5-jvm:4.2.0")
	testImplementation("io.kotest:kotest-assertions-core-jvm:4.2.0")
	testImplementation("io.kotest:kotest-property-jvm:4.2.0")
	testImplementation("com.beust:klaxon:4.0.2")
	testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")
	testImplementation("org.springframework.boot:spring-boot-starter-test") {
		exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
	}
	testImplementation("org.apache.hbase:hbase-client:1.4.13")
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
	create("unit") {
		java.srcDir(file("src/test/kotlin"))
		compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
		runtimeClasspath += output + compileClasspath
	}
}

tasks.register<Test>("unit") {
	description = "Runs the unit tests"
	group = "verification"
	testClassesDirs = sourceSets["unit"].output.classesDirs
	classpath = sourceSets["unit"].runtimeClasspath

	//copy all env vars from unix/your test container into the test
	setEnvironment(System.getenv())
	//to copy individual ones do this
	//environment("ABC", System.getEnv("ABC"))

	testLogging {
		outputs.upToDateWhen {false}
		showStandardStreams = true
		exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
		events = setOf(org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED, org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED, org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED)
	}
}
