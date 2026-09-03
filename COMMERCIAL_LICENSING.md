> **Draft status:** This is a first draft, written to reflect the terms discussed and agreed on with the maintainer. It is **not legal advice** and has not been reviewed by a lawyer. Before this is used to actually collect a signature or a payment from a real customer, it should be reviewed by a lawyer qualified in the governing jurisdiction (Section 13.1). Treat this as a strong starting point, not a finished contract.

# Thryve Commercial License Agreement

This Thryve Commercial License Agreement ("**Agreement**") is entered into between **Youssef Mostafa Abdellatif** ("**Licensor**," "**We**," "**Us**") and the licensee identified in the applicable Order Form ("**Licensee**," "**You**"), effective as of the date set forth in that Order Form (the "**Effective Date**").

This Agreement is offered as an alternative to the AGPL-3.0-or-later license under which Thryve is otherwise made available (see [`LICENSE`](LICENSE)). It allows Licensee to use the Licensed Software in a closed-source product without the source-disclosure obligations of the AGPL. By signing an Order Form that references this Agreement, Licensee agrees to be bound by its terms.

---

## 1. Definitions

**"Licensed Software"** means Thryve — including the `thryve`, `thryve_core`, `thryve_ffi`, `thryve_observers`, `thryve_run`, and `thryve_utils` crates, any associated header files and redistributable binaries, and any future crates or components released as part of the Thryve project — in source or object code form, as made available by Licensor.

**"Licensed Project"** means the specific software product or application described in the applicable Order Form, into which Licensee directly integrates the Licensed Software. Depending on the License Tier specified in the Order Form (see Section 4), "Licensed Project" may refer to a single named product, a specific list of named products, or, under the Unlimited Tier, any product Licensee develops; where more than one product is covered, each reference elsewhere in this Agreement to "the Licensed Project" applies individually to each of them. A Licensed Project remains the same Licensed Project across subsequent versions, rewrites, refactors, rebrands, and renames, for as long as it continues to represent substantially the same underlying product or commercial offering (see Section 5.1).

**"License Tier"** means the scope of the license granted under this Agreement, as specified in the Order Form: Single-Project, Multi-Project, or Unlimited (see Section 4).

**"Application"** means a compiled, distributable software product created by Licensee that incorporates the Licensed Software as part of the Licensed Project.

**"Customer(s)"** means end users or customers of Licensee to whom Licensee distributes an Application.

**"Distribute"** means to provide copies of an Application — including the Licensed Software as incorporated within it — to Customers, whether by download, physical media, SaaS delivery, or any other means.

**"Modifications"** means any modification, adaptation, or derivative work of the Licensed Software created by or for Licensee.

**"Order Form"** means a signed document referencing this Agreement that specifies the Licensee, the License Tier, the Licensed Project(s), the Fee, and the Effective Date. The Order Form is not reproduced in this repository; to obtain one, submit a proposal at [thryve-framework.com](https://thryve-framework.com).

**"Prohibited Use"** has the meaning given in Section 10.1.

## 2. License Grant

**2.1 Development and Use.** Subject to the terms of this Agreement and payment of the Fee, Licensor grants Licensee a perpetual (subject to Section 10), worldwide, non-exclusive, non-transferable license to use, modify, and create Modifications of the Licensed Software solely for the purpose of developing, testing, and maintaining the Licensed Project.

**2.2 Distribution.** Licensee may incorporate the Licensed Software, including any Modifications, into the Licensed Project as an Application, and Distribute that Application to Customers, without any further license, fee, or obligation on the part of such Customers under this Agreement solely as a result of their use of the Application.

**2.3 Updates.** For as long as Licensor continues to make updates to the Licensed Software generally available, Licensee may use such updates within the scope of this Agreement at no additional fee. Licensor is under no obligation to release any particular update, fix, or new version.

**2.4 Scope Is the Direct Integration, Not Everything Built On Top of It.** This license extends to the Licensed Project as the software that directly integrates the Licensed Software (for example, via the `thryve_ffi` C interface, or as a direct Rust dependency). Where the Licensed Project is itself a platform, engine, or product upon which Licensee's own Customers or third-party developers build further software *without themselves directly integrating the Licensed Software*, those Customers or third-party developers are not required to hold a separate license under this Agreement.

## 3. Restrictions

Licensee shall not:

(a) distribute, sublicense, or otherwise make available the Licensed Software in source code form, or as a standalone SDK or library separable from the Application, to any third party;

(b) use the Licensed Software to develop, or incorporate it into, any product other than a Licensed Project covered by the Order Form, without executing an additional or amended Order Form covering that product (see Section 4);

(c) remove or alter any copyright, trademark, or proprietary notice contained in the Licensed Software; or

(d) use the Licensed Software, or permit the Licensed Project to be used, for a Prohibited Use (Section 10.1).

## 4. License Tiers

**4.1 Single-Project Tier.** Under the Single-Project Tier, the Order Form licenses the Licensed Software for use in exactly one Licensed Project. This is the standard tier.

**4.2 Multi-Project Tier.** Under the Multi-Project Tier, the Order Form licenses the Licensed Software for use in each Licensed Project specifically listed in it. Licensee may cover further projects only by executing an amended or additional Order Form listing them, for an additional Fee agreed between the parties.

**4.3 Unlimited Tier.** The Unlimited Tier is a discretionary grant, not a purchasable option. Licensor may, at Licensor's sole and unfettered discretion, grant a small number of parties an Unlimited Tier license covering any and all products or applications Licensee develops, without limitation to a named Licensed Project — for example, in recognition of significant contributions to the Thryve project. The Unlimited Tier is not available for purchase, is not offered upon request, and nothing in this Agreement entitles any Licensee to negotiate for or expect one.

## 5. Project Continuity; Anti-Circumvention

**5.1** Ordinary evolution of a Licensed Project — rewrites, refactors, rebrands, renames, new major versions — does not create a new project, so long as it remains substantially the same underlying product or commercial offering.

**5.2** Licensee shall not restructure, split, rename, or otherwise reorganize a Licensed Project, or create what is in substance a continuation of a Licensed Project, for the purpose of avoiding the scope or Fees of this Agreement. Any such reorganization is deemed to remain part of, and subject to the Fees applicable to, the original Licensed Project.

**5.3** No more than once every twelve (12) months, Licensor may request, and Licensee shall provide within a reasonable time, a brief written description of the Licensed Project(s) in which the Licensed Software is then in use. This Section 5.3 does not apply to a Licensee under the Unlimited Tier.

## 6. Fees

**6.1** Licensee shall pay the Fee specified in the applicable Order Form for the License Tier granted. Unless the Order Form states otherwise, the Fee is a single, one-time payment, and the license granted under Section 2 is perpetual, subject to Section 10.

**6.2** Licensor may, at its sole discretion, waive or reduce the Fee for a given Order Form, including when granting the Unlimited Tier under Section 4.3. A waived or reduced Fee does not otherwise affect the enforceability of this Agreement.

**6.3** Fees are non-refundable except as required by applicable law.

## 7. Ownership

Licensor retains all right, title, and interest in and to the Licensed Software, including all intellectual property rights in it. No rights are granted except as expressly set out in this Agreement. Licensee owns the Modifications it creates, subject always to Licensor's underlying rights in the Licensed Software those Modifications are based on.

## 8. Warranty Disclaimer

THE LICENSED SOFTWARE IS PROVIDED "AS IS," WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, TITLE, AND NON-INFRINGEMENT. LICENSOR DOES NOT WARRANT THAT THE LICENSED SOFTWARE WILL BE ERROR-FREE OR OPERATE UNINTERRUPTED. ANY SUPPORT LICENSOR CHOOSES TO PROVIDE IS ON A BEST-EFFORT BASIS ONLY; LICENSOR MAKES NO COMMITMENT TO ANY SPECIFIC RESPONSE TIME OR SERVICE LEVEL.

## 9. Limitation of Liability

**9.1** EXCEPT FOR (I) LICENSEE'S BREACH OF SECTION 3 (RESTRICTIONS) OR SECTION 10.1 (PROHIBITED USE), (II) EITHER PARTY'S GROSS NEGLIGENCE OR WILLFUL MISCONDUCT, AND (III) LICENSEE'S INDEMNIFICATION OBLIGATIONS UNDER SECTION 11, TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, IN NO EVENT SHALL EITHER PARTY BE LIABLE FOR ANY INDIRECT, INCIDENTAL, SPECIAL, CONSEQUENTIAL, OR PUNITIVE DAMAGES, OR ANY LOSS OF PROFITS, REVENUE, DATA, OR BUSINESS OPPORTUNITY, ARISING OUT OF OR RELATED TO THIS AGREEMENT.

**9.2** EXCEPT FOR THE CARVE-OUTS IN SECTION 9.1, LICENSOR'S TOTAL AGGREGATE LIABILITY ARISING OUT OF OR RELATED TO THIS AGREEMENT SHALL NOT EXCEED THE TOTAL FEES ACTUALLY PAID BY LICENSEE TO LICENSOR UNDER THE APPLICABLE ORDER FORM.

## 10. Prohibited Use; Termination

**10.1 Prohibited Uses.** Licensee shall not use the Licensed Software, or permit the Licensed Project to use it, for:

(a) the design, development, or operation of weapons systems, munitions, or military targeting systems;

(b) any activity that facilitates genocide, torture, or other serious violation of human rights; or

(c) any activity that is illegal under the law applicable to where it is conducted.

**10.2 Immediate Termination.** Licensor may terminate this Agreement and all licenses granted under it immediately upon written notice, without any cure period, if Licensor determines, in its sole reasonable discretion based on credible evidence, that Licensee has engaged in a Prohibited Use or has materially breached any restriction in Section 3. 

**10.3 Termination for Other Breach.** For any other material breach of this Agreement, either party may terminate upon thirty (30) days' written notice if the breaching party fails to cure the breach within that period.

**10.4 Effect of Termination.** Upon termination, all licenses granted under Section 2 end immediately, and Licensee must stop all further use, development, and distribution of the Licensed Software going forward. Termination does not require Licensee to recall Applications already lawfully distributed to Customers before the termination date, and does not affect those Customers' ability to keep using copies they already have. Sections 6.3, 7, 8, 9, 11, 12, and 13 survive termination.

## 11. Indemnification

Licensee shall defend, indemnify, and hold harmless Licensor against any claims, damages, liabilities, costs, and expenses (including reasonable attorney's fees) arising out of or related to: (a) Licensee's Application or Licensed Project; (b) Licensee's use of the Licensed Software in breach of this Agreement; or (c) a Prohibited Use by Licensee. Licensor does not indemnify Licensee against any third-party claims, including intellectual property claims.

## 12. Confidentiality

Each party shall keep confidential the terms of the applicable Order Form (including the Fee) and shall not disclose them to third parties without the other party's consent, except as required by law or to professional advisors bound by confidentiality.

## 13. General

**13.1 Governing Law.** This Agreement is governed by the laws of the Arab Republic of Egypt, excluding its conflict-of-law provisions.

**13.2 Entire Agreement.** This Agreement, together with the applicable Order Form, is the entire agreement between the parties regarding its subject matter, and supersedes any prior discussions or agreements about it.

**13.3 Assignment.** Licensee may not assign this Agreement without Licensor's prior written consent, except in connection with a merger, acquisition, or sale of substantially all assets relating to the Licensed Project.

**13.4 Severability.** If any provision of this Agreement is held unenforceable, the remaining provisions remain in full effect, and the unenforceable provision will be replaced with one that most closely achieves its original intent.

**13.5 No Waiver.** A party's failure to enforce any provision of this Agreement is not a waiver of its right to do so later.

**13.6 Notices.** Notices under this Agreement must be in writing and sent to the contact details specified in the applicable Order Form, or otherwise to [thryve-framework.com](https://thryve-framework.com) for notices to Licensor.

---

## Exhibit A: Order Form

The Order Form is executed individually with each Licensee and is not reproduced in this repository. To request one, submit a proposal at [thryve-framework.com](https://thryve-framework.com).
