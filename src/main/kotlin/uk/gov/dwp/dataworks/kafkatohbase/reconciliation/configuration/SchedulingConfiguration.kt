package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.stereotype.Component

@Component
@Profile("!test")
@EnableScheduling
class SchedulingConfiguration
