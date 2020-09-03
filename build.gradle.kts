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

	environment("CONTAINER_VERSION", "latest")
	environment("ENVIRONMENT", "local-dev")
	environment("APPLICATION_NAME", "reconciliation")
	environment("APP_VERSION", "test")
	environment("LOG_LEVEL", "DEBUG")
	environment("SECRETS_REGION", "eu-west-2")
	environment("SECRETS_METADATA_STORE_PASSWORD_SECRET", "metastore_password")
	environment("HBASE_TABLE_PATTERN", "([-\\w]+)\\.([-\\w]+)")
	environment("HBASE_ZOOKEEPER_PARENT", "/hbase")
	environment("HBASE_ZOOKEEPER_PORT", "2181")
	environment("HBASE_ZOOKEEPER_QUORUM", "hbase")
	environment("HBASE_RPC_TIMEOUT_MILLISECONDS", "1200")
	environment("HBASE_CLIENT_TIMEOUT_MS", "1200")
	environment("HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD_MS", "12000")
	environment("HBASE_OPERATION_TIMEOUT_MILLISECONDS", "1800")
	environment("HBASE_PAUSE_MILLISECONDS", "50")
	environment("HBASE_RETRIES", "3")
	environment("METADATASTORE_USER", "reconciliationwriter")
	environment("METADATASTORE_PASSWORD_SECRET_NAME", "metastore_password")
	environment("METADATASTORE_DUMMY_PASSWORD", "my-password")
	environment("METADATASTORE_DATABASE_NAME", "metadatastore")
	environment("METADATASTORE_ENDPOINT", "metadatastore")
	environment("METADATASTORE_PORT", "3306")
	environment("METADATASTORE_TABLE", "ucfs")
	environment("METADATASTORE_CA_CERT_PATH", "/certs/AmazonRootCA1.pem")
	environment("METADATASTORE_QUERY_LIMIT", "20")
	environment("METADATASTORE_USE_AWS_SECRETS", "false")
	environment("SPRING_PROFILES_ACTIVE", "DUMMY_SECRETS")

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
