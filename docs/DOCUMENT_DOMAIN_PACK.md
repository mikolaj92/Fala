# Document Domain Pack

Fala 2.0 core starts with `Carrier`. Document workflows are modeled by the
`fala.domain_packs.documents` compatibility pack.

The pack maps document concepts onto Carrier-first runtime objects:

- `DocumentCarrierInput.id` becomes `Carrier.id`.
- `document_type` becomes the Carrier type suffix, for example
  `document.invoice_document`.
- `title`, `relation`, `media_type`, `source_uri`, `values`, and `artifacts`
  live in `Carrier.payload`.
- document metadata stays in `Carrier.metadata` with
  `domain_pack: documents`.
- `document_observation` emits a `document.accepted` observation.
- `document_projection` creates a document read-model projection keyed by
  `document:{carrier_id}`.

New core code should accept arbitrary Carrier types such as `arbitration_case`,
`sensor_reading`, or `payment_event` without document fields. Use the document
domain pack only when the workflow is explicitly about documents.
