# Known limitations

- Does not send email.
- Template placeholders `street_address_1/2` don't exist in the proto.
- Recipient addresses are logged.
- Health stays `SERVING` during the 2 s shutdown drain (it is static); the pod is taken out of the Service by Kubernetes on termination, not by the probe.
- `requirements.txt` was compiled for Python 3.13 (`--python-version 3.13`) while the image runs 3.12; it installs and runs there (verified by the image build and the local test run on Python 3.12), but the lock was not recompiled for 3.12.
