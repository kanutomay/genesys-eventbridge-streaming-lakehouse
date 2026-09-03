# Privacy and Retention Considerations

## Scope

This repository demonstrates a technical architecture for ingesting, transforming, cataloging, retaining, and querying contact-center event data. It is not a complete organizational privacy or records-management program and should not be interpreted as legal advice or certification of compliance with Panama's Ley 81—or any other data-protection framework.

## Retention rationale

The representative S3 lifecycle retains data for seven years. This period originated from an organization-approved retention requirement reviewed and directed by accountable management; it was not selected solely for engineering convenience.

Organizations adopting this pattern should independently determine whether the same period is appropriate for each data class. That assessment should consider:

- Applicable laws, regulations, contracts, and legal-hold requirements
- The documented purposes for which the data is processed
- Whether identifiable data remains necessary throughout the retention period
- Whether shorter retention, pseudonymization, or anonymization is appropriate for raw events, transcripts, and analytical datasets

## Downstream-copy responsibilities

The S3 lakehouse is a separate processing system from Genesys Cloud CX. Retention, correction, deletion, or archival performed in the source platform does not automatically propagate to the lakehouse.

An operational deployment should therefore define how applicable requests and controls are handled across both systems, including:

- Access, correction, deletion, opposition, and portability requests
- Legal holds and exceptions to scheduled deletion
- Identity resolution across raw, transformed, curated, and backup records
- Backup and error-prefix handling
- Audit logging and evidence of completed actions

These organization-wide workflows were outside the scope of the implementation represented in this repository.

## Required organizational controls

A production deployment should be supported by governance and operational controls appropriate to its jurisdiction and risk profile, including:

- A documented lawful basis and processing purpose
- Privacy notices and consent management where applicable
- Role-based access and periodic access reviews
- Encryption and key-management standards
- Data-classification and minimization rules
- Incident detection, escalation, and notification procedures
- Cross-border transfer assessment where applicable
- Periodic review of retention schedules and processing necessity

The infrastructure in this repository can support such a program, but infrastructure alone does not establish regulatory compliance.
