# Contour Split Migration Note

## Already Separated Now

- The host layout now reserves distinct contour roots for `df-dev` and `ownerbox`.
- Path ownership is defined in `config/contour_policy.json`.
- OwnerBox has placeholder domain metadata, task namespace, memory namespace, and artifact placeholders without feature logic.

## Still Shared

- DF Core code and control remain in `/home/avoro/projects/digital_foreman`.
- Shared config, adapters, and future storage adapter responsibilities remain part of DF Core.
- No runtime storage adapter or sync implementation is introduced in this step.

## Next Correct Step

- Introduce a small runtime path resolver or environment binding that reads the contour policy and routes contour-private artifacts and memory to the correct roots.
- Keep that step limited to path selection and boundary enforcement, not business logic.

## Why Storage Layering Comes After Contour Separation

- Storage policy is only meaningful after domain ownership is explicit.
- If storage is added before contour boundaries are clear, owner and dev state can leak through shared paths and force a later rewrite.
- The contour split establishes the contract that a future storage adapter must respect.
