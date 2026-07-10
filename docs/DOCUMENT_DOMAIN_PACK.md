# Document Domain Pack

Fala core starts with `Impulse`. Document workflows are modeled by the
`fala.domain_packs.documents` domain pack.

The pack maps document concepts onto Impulse-first runtime objects:

- `DocumentImpulseInput.id` becomes `Impulse.id`.
- `document_type` becomes the Impulse type suffix, for example
  `document.invoice_document`.
- `title`, `relation`, `media_type`, `source_uri`, `values`, and `reactions`
  live in `Impulse.payload`.
- document metadata stays in `Impulse.metadata` with
  `domain_pack: documents`.
- `document_association` emits a `document.accepted` association.
- `document_projection` creates a document read-model projection keyed by
  `document:{impulse_id}`.

New core code should accept arbitrary Impulse types such as `arbitration_case`,
`sensor_reading`, or `payment_event` without document fields. Use the document
domain pack only when the workflow is explicitly about documents.
