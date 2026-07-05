import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.serialization)
}

android {
    namespace = "me.rerere.p2p"
    compileSdk = 37

    defaultConfig {
        minSdk = 26
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        consumerProguardFiles("consumer-rules.pro")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    tasks.withType<KotlinCompile>().configureEach {
        compilerOptions.optIn.add("kotlin.time.ExperimentalTime")
    }
}

dependencies {
    implementation(project(":common"))

    // okhttp
    api(libs.okhttp)
    api(libs.okhttp.sse)
    api(libs.okhttp.logging)

    // kotlinx
    api(libs.kotlinx.serialization.json)
    api(libs.kotlinx.coroutines.core)

    // WebRTC Android AAR (§3.3.1). Replace with the chosen trusted vendor
    // build before release; the version is a placeholder for development.
    api(libs.webrtc.android)

    // tests
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
}
