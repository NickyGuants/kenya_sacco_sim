# Operational Blueprint for Building KENYA_SACCO_SIM: A High-Fidelity Synthetic AML Dataset for Kenyan SACCOs

This dossier is written for engineers, not for general readers. It treats Kenya as a mobile-money-first, payroll-linked, SACCO-heavy retail finance environment in which value moves across wallets, banks, agents, employer check-off files, paybills, tills, remittance corridors, and cash. The highest-confidence evidence base in this pass comes from the Central Bank of Kenya, SASRA, KNBS, FinAccess, the Financial Reporting Centre, FATF, ESAAMLG, KRA, and public product literature from major SACCO operators. Where public sources do not expose the micro-distributions needed for simulation, I mark the result as a modelling prior rather than an observed fact. citeturn14search0turn24view1turn39search4turn39search6turn46search1

A guiding design choice for KENYA_SACCO_SIM should be this: do **not** model a SACCO as an isolated ledger. Model it as a node inside an ecosystem dominated by wallet usage, salary periodicity, school-fee financing, member guarantees, paybill funding, and opportunistic cash conversion. That is the only way to make alerts, false positives, and laundering scenarios look Kenyan rather than generic. citeturn16view1turn16view2turn29search5turn33search2

## Payment rails and operational observables

entity["company","Safaricom PLC","telecom kenya"]’s M-Pesa is the centre of gravity in retail payments, but the simulator should represent a broader wallet layer that also includes entity["company","Airtel Kenya","telecom kenya"] wallets, bank-wallet interoperability, paybills, tills, and agent-assisted cash conversion. As of December 2024, the CBK mobile-payments series showed about 82.4 million registered mobile payment users, roughly 375,000 agents, 2.58 billion monthly transactions, and KSh 753.4 billion in monthly value; across 2024 the monthly value band sat roughly between KSh 670.5 billion and KSh 790.8 billion. Separately, the CBK-financial-stability report recorded 36.9 million active 30-day mobile-money customers at December 2024, up from 31.4 million a year earlier. citeturn2view0turn13search9

The household-use pattern is what makes wallets structurally important for simulation. FinAccess 2024 found that 82.3 percent of adults used mobile money, and 52.6 percent used it **daily**. By contrast, banks and SACCOs are more important for monthly obligations than for day-to-day liquidity: daily bank use was 4.8 percent, daily mobile-banking use 8.4 percent, while monthly bank usage was 58.7 percent and monthly SACCO usage 74.9 percent. That means normal Kenyan retail behaviour should show very frequent wallet traffic layered on top of lower-frequency but economically heavier bank and SACCO events. citeturn16view3turn16view4

For KENYA_SACCO_SIM, the mobile-money layer should therefore include at least six retail event families: wallet-to-wallet P2P; paybill wallet-to-business; till wallet-to-merchant; agent cash-in; agent cash-out; and wallet-bank transfers. Even when a member “uses M-Pesa”, the economic destination may be a bank or SACCO account because many SACCOs accept M-Pesa paybill deposits into FOSA savings, prime accounts, deposits, or shares. Public SACCO product pages show this clearly: Unaitas accepts deposits via M-Pesa paybill 544700 into named accounts; Stima uses paybill 823244 with encoded references for monthly deposits, share capital, and prime accounts; Kenya Police SACCO accepts monthly contributions through employer check-off, standing order, paybill 4027903, and counter cash. citeturn32search0turn33search7turn33search2

Instant account-to-account transfer must also exist as a distinct rail rather than being collapsed into generic “bank transfer”. Public SACCO interfaces show PesaLink exposure at the member edge. Unaitas exposes PesaLink over USSD with real-time send/receive capability, a per-transaction cap of KES 300,000 and daily cap of KES 999,999, and Stima exposes PesaLink through app and USSD as well. This matters because PesaLink-like traffic is operationally different from ACH/EFT: it is real-time, individually initiated, and more useful for pass-through laundering and “salary lands in bank, exits instantly” behaviour. citeturn32search0turn33search7

Large-value banking flows should be modelled separately from retail instant transfers. KEPSS, Kenya’s RTGS backbone, processed more than 5.3 million local-currency transactions worth more than KSh 45 trillion between August 2023 and August 2024. In Q2 2024 it handled 2.1 million messages worth KSh 10.6 trillion, and in Q3 2024 2.10 million messages worth KSh 11.2 trillion, implying an average payment size a little above KSh 5 million. In SACCO simulation terms, RTGS is not the member’s normal daily rail; it is for treasury movements, supplier payments, institutional settlements, and large-value transfers. citeturn47search0turn47search1turn47search4

ACH/EFT still matters because payroll, interbank credits, supplier payments, and scheduled deductions do not all move through instant rails. The CBK statistical bulletin publishes monthly debit and credit EFT article counts and values, confirming that ACH remains a meaningful channel alongside wallets and RTGS. For simulator purposes, use ACH/EFT for payroll batches, standing orders, biller settlement, and slow interbank credits, and reserve RTGS for treasury or very large-value cases. citeturn13search4turn8view0turn8view1

Cash must be retained as a first-class rail. Kenya’s digitalisation is real, but the cash loop is not gone; rather, it is mediated through mobile-money agents, branches, and over-the-counter funding. The CBK’s AML evaluation noted that mobile wallets can be funded by cash deposit at a registered agent, including third-party deposit, and that the third-party making the deposit is not necessarily identified, which raises monitoring risk. FinAccess also shows the persistence of branch and traditional-channel preference in SACCO usage, especially in rural contexts. citeturn44view1turn16view2

Cross-border remittances belong in the baseline, not only in fraud injection. CBK reported remittance inflows of USD 4.945 billion in 2024, with the United States contributing 51 percent of inflows. The 2021 diaspora-remittance survey found that the most-used service-provider classes for sending cash to Kenya were mobile-money operators, money-transfer companies, and banks, and that M-Pesa/Safaricom was the single most-preferred provider at 20 percent. It also found that dominant remittance providers were typically chosen for convenience, access, speed, and charges; cost ranges were about 4–5 percent for dominant providers, with North America-, East Africa-, Oceania-, and LAC-to-Kenya corridors generally cheaper than Europe- or Asia-to-Kenya corridors. citeturn34search2turn34search12turn36view0turn36view1turn35view0

Government flows should be modelled in four buckets: payroll, social transfers, tax collections, and service fees. KRA accepts tax payments through iTax-generated references over partner banks and M-Pesa paybill 222222, with some tax products such as turnover/presumptive-related flows also using paybill 572572 in published notices. The government’s eCitizen/Pesaflow environment supports M-Pesa, Airtel Paybill, bank channels, card, PesaLink, and RTGS. For benefits, the Ministry of Labour’s social-protection framework documents an account-based, bank-contracted, biometric/card-assisted payment model for Inua Jamii cash transfers. These flows are operationally different from consumer retail spending: they are reference-driven, often batch-originated, and usually more periodic. citeturn37search1turn37search2turn37search8turn37search9turn38search1turn38search3turn37search0

### Recommended canonical transaction fields by rail

The table below is an engineering recommendation rather than a regulatory schema. It translates the public rails into machine-observable events for synthetic generation.

| Rail family | Minimum observable fields |
|---|---|
| Wallet P2P | `txn_id, ts, rail=wallet_p2p, provider, sender_msisdn_hash, receiver_msisdn_hash, amount, fee, origin_channel, geo_hint, balance_after` |
| Wallet paybill / till | `txn_id, ts, rail=wallet_billpay, provider, payer_msisdn_hash, business_ref, paybill_or_till, merchant_class, amount, fee, linked_account_id(optional)` |
| Agent cash-in / cash-out | `txn_id, ts, rail=agent_cash, provider, agent_id, member_msisdn_hash, amount, direction, agent_location, source_of_funds_flag(optional)` |
| Bank instant | `txn_id, ts, rail=instant_bank, scheme, dr_acct, cr_acct, amount, fee, bank_id, narration, device_id` |
| ACH / EFT | `batch_id, entry_id, file_date, value_date, dr_bank, cr_bank, account_ref, amount, debit_credit, employer_or_biller_ref` |
| RTGS / KEPSS | `txn_id, ts, rail=rtgs, ordering_institution, beneficiary_institution, amount, purpose_code, corporate_flag, treasury_flag` |
| Standing order / check-off | `instruction_id, run_date, employer_id or source_bank_id, member_id, sacco_account_id, amount, status, reject_reason` |
| Remittance | `txn_id, ts, rail=remittance, corridor_country, source_provider, receive_mode(wallet/bank/cash_pickup), beneficiary_member_id(optional), fx_amount, fx_rate, ksh_amount` |
| Government / tax / eCitizen | `txn_id, ts, rail=gov_payment, agency_code, service_code, prn_or_reference, amount, payment_mode, payer_type(individual/business)` |

**HOW THIS FEEDS KENYA_SACCO_SIM:** build rails first, then map every synthetic transaction to one rail family and one economic purpose. The simulator should never emit a generic “transfer” without a rail-specific origin, counterparty class, settlement mode, and reference structure.

## SACCO operating model and ledger semantics

entity["organization","SACCO Societies Regulatory Authority","nairobi, kenya"] distinguishes between deposit-taking SACCOs and specified non-deposit-taking SACCOs. Deposit-taking SACCOs operate FOSA-like transactional services and can offer withdrawable savings accounts similar to banks; specified non-deposit-taking SACCOs are effectively BOSA-only institutions in which member deposits are used as collateral for credit and are ordinarily refundable on exit rather than through day-to-day withdrawal. SASRA’s own public description is explicit that it licenses deposit-taking business and supervises both deposit-taking and specified non-deposit-taking SACCOs. It currently reports 177 DT SACCOs and 178 authorised non-withdrawable/non-deposit-taking SACCOs. citeturn30search2turn30search3

The aggregate scale is large enough that SACCO behaviour cannot be treated as niche. In 2023, SASRA reported 357 regulated SACCOs with 6.84 million members, KSh 971.96 billion in assets, KSh 682.19 billion in deposits, and KSh 758.57 billion in gross loans. Concentration is material: 53 regulated SACCOs held 73.34 percent of assets, and 39 held 65.27 percent of deposits. For simulation, that means you should not generate a flat landscape of identically sized institutions; you need a fat-tailed institution-size distribution with a few very large SACCOs and many smaller ones. citeturn29search5

FinAccess 2024 also confirms that SACCO usage is not static and not purely branch-based. Overall SACCO usage rose to 11.7 percent of adults in 2024 from 9.6 percent previously, and mobile SACCO channels overtook traditional usage in aggregate, though rural members still leaned more toward traditional interaction than urban members. That tells you that a realistic SACCO simulator must support both branch-heavy and digitised institutions, and that rural/urban channel preference should be a member-level parameter. citeturn16view2

The simplest and most important product distinction is this:

- **Share capital** is ownership capital. In public SACCO FAQs it is described as non-refundable on demand and ordinarily only transferable or saleable on exit.
- **Deposits** are regular member contributions that drive loan eligibility and are usually locked as long-term member savings.
- **BOSA** is the back-office saving-and-credit core where non-withdrawable deposits, guarantees, and loan eligibility live.
- **FOSA** is the transactional front office where salary accounts, current/savings accounts, instant advances, debit/ATM access, and wallet-linked activity sit. citeturn33search2turn30search0turn30search2

### Product families that must exist in the simulator

Public product menus from entity["organization","Mwalimu National DT SACCO","teachers sacco kenya"], entity["organization","Kenya National Police DT SACCO","police sacco kenya"], entity["organization","Stima DT Sacco","energy workers sacco"], and entity["organization","Unaitas","deposit taking sacco kenya"] show a common stack: long-term BOSA loans; school-fee and emergency facilities; salary-linked FOSA advances; business/biashara loans; asset finance; call/fixed deposits; children, school, company, and church accounts; instant mobile loans; ATM/debit access; app/USSD/mobile-banking interfaces; PesaLink or bank transfer; and M-Pesa paybill funding. citeturn33search0turn33search2turn33search8turn32search0

From a ledger-design standpoint, those products should appear as the following event types:

| Business process | Ledger appearance in raw data |
|---|---|
| Member onboarding | `MEMBERSHIP_FEE`, `SHARE_CAPITAL_INITIAL`, `DEPOSIT_OPENING`, `KYC_CAPTURED`, `EMPLOYER_LINKED(optional)` |
| Monthly check-off | `CHECKOFF_CREDIT` into deposits/share capital/FOSA prime with `employer_batch_id` |
| Voluntary top-up | `CASH_DEPOSIT_BRANCH`, `MPESA_PAYBILL_TO_FOSA`, `STANDING_ORDER_IN`, `PESALINK_IN` |
| Share-capital increase | `SHARE_CAPITAL_TOPUP` |
| BOSA loan application | `LOAN_APPLICATION`, `GUARANTOR_PLEDGE`, `AFFORDABILITY_CHECK`, `EMPLOYER_CONFIRMATION(optional)` |
| BOSA loan disbursement | `LOAN_DISBURSEMENT_BOSA` then either `INTERNAL_TRANSFER_TO_FOSA`, `BANK_EFT_OUT`, `WALLET_DISBURSEMENT`, or `CHEQUE_ISSUE` |
| FOSA instant advance | `LOAN_DISBURSEMENT_FOSA_INSTANT`, `MOBILE_LOAN_FEE`, `AUTO_RECOVERY` |
| Loan repayment | `CHECKOFF_LOAN_RECOVERY`, `SALARY_SWEEP`, `MPESA_LOAN_REPAYMENT`, `CASH_LOAN_REPAYMENT`, `PENALTY_ACCRUAL`, `INTEREST_ACCRUAL` |
| Exit / withdrawal | `ACCOUNT_CLOSURE_REQUEST`, `DEPOSIT_REFUND`, `SHARE_TRANSFER_OUT`, `LOAN_OFFSET_AT_EXIT` |
| Dormancy / reactivation | `ACCOUNT_DORMANT_FLAG`, `REACTIVATION_FEE(optional)`, `KYC_REFRESH`, `PIN_RESET`, `FIRST_CREDIT_AFTER_DORMANCY` |

The strongest hard evidence for loan mechanics in public SACCO documentation comes from Kenya Police SACCO’s FAQ. It states that member deposits determine loan eligibility, that monthly contributions can come through employer check-off, standing order, M-Pesa paybill, or cash deposit, that loans are secured by deposits plus guarantors, that guarantors should be active members with at least six months of deposit contribution, and that members can borrow up to three times savings subject to the government two-thirds net-pay rule after deductions. It also notes that a portion of a loan can be used to “boost” deposits so the member qualifies for a larger loan. That is exactly the kind of recursive behaviour that generic loan simulators miss. citeturn33search2

That single source is enough to justify four SACCO-specific state variables in the simulator that ordinary bank datasets often do not need:

1. `deposit_multiple_limit`
2. `guarantor_capacity_remaining`
3. `payroll_attachable_income`
4. `deposit_boost_flag` / `boost_from_loan_fraction` citeturn33search2

Digital service delivery is no longer optional. Stima exposes app, USSD, ATM, PesaLink, M-Pesa-to-SACCO deposit, and agent-like cash access through partner infrastructure. Unaitas exposes M-Pesa deposit, Mo-Cash advance, PesaLink, ATM cards, MoneyGram, and diaspora account functionality. Kenya Police SACCO exposes M-TAWI over USSD and app, and Mwalimu advertises app/USSD, ATM, Lipa na Mwalimu, and both BOSA and FOSA loan menus. This is enough evidence to model SACCO tech maturity as a categorical institution-level parameter rather than assuming a uniform core stack. citeturn33search7turn33search8turn32search0turn33search5turn33search0

### Institution archetypes for simulation

Use these institution archetypes, then assign members to them:

| Archetype | Typical funding | Transactional richness | Loan style | AML exposure |
|---|---|---|---|---|
| Teacher / public-sector DT SACCO | payroll check-off | medium-high | stable multipliers, school-fee cycles | payroll proxy abuse, guarantor rings |
| Uniformed-services DT SACCO | payroll check-off + cash top-ups | medium | salary-backed, guarantor-heavy | identity misuse, insider collusion |
| Utility / formal private-sector DT SACCO | salary + digital channels | high | FOSA-heavy + instant credit | rapid pass-through, channel abuse |
| Community / faith / chama-linked SACCO | cash + wallet + deposits | low-medium | smaller, relationship-based | source-of-funds opacity |
| Farmer / cooperative BOSA focus | seasonal produce cashflows | low-medium | harvest-sensitive | seasonal arrears, external cash injection |
| SME / biashara SACCO | mixed wallet, till, cash | high | biashara and asset finance | merchant-funding wash flows |
| Diaspora-facing SACCO | remittance-linked | high | property / investment / savings | remittance layering, mule beneficiaries |

**HOW THIS FEEDS KENYA_SACCO_SIM:** model institutions first, then products. A teacher SACCO, a police SACCO, and a community SACCO can share the same table schema, but they should differ in funding rails, loan eligibility, digital-channel mix, guarantee density, and normal transaction cadence.

## Persona flow maps and baseline behaviours

The persona layer is where the rail model and SACCO model meet. For formal salaried personas, the best macro anchor available in this pass is KNBS’s 2025 Economic Survey statement that average annual public-sector earnings rose to KSh 933.1 thousand in 2024, which is about KSh 77.8 thousand per month before deductions. FinAccess then tells us that daily liquidity is predominantly mobile-money based, while monthly obligations are frequently handled through banks and SACCOs. I therefore recommend using the KNBS public-sector mean as the centre of the formal-worker prior, then spreading cohorts around it by employer type and salary grade. citeturn52search0turn16view4

The table below mixes observed system facts with engineering priors. Values marked as ranges are **simulation priors**, not official microdata.

| Persona | Monthly income source | Main rails | Saving / borrowing pattern | Cash habit | Likely balances | Realistic transaction count per month |
|---|---|---|---|---|---|---|
| Salaried teacher SACCO member | payroll, centred around public-sector mean; prior KSh 45k–110k | employer check-off, bank/FOSA, M-Pesa paybill, wallet P2P | mandatory deposits monthly; school-fee or development loan; occasional FOSA advance | moderate cash-out near payday and school opening | FOSA KSh 2k–25k end-month; BOSA deposits accumulate slowly | 12–30 ledger events; 25–70 total financial touches incl. wallet |
| County worker | payroll, prior KSh 35k–95k | payroll to bank/FOSA, standing orders, M-Pesa | deposits + emergency loan + development loan; some irregular county delays scenario | medium cash dependence if county payment delays | lower average free balance than teacher cohort | 10–26 ledger events; 20–60 total |
| SME owner using SACCO + M-Pesa | business receipts, prior KSh 35k–250k highly volatile | till/paybill, wallet P2P, cash deposit, PesaLink, biashara loan | irregular savings, higher working-capital borrowing, frequent internal transfers | high cash recycling | transactional account often oscillates near zero to KSh 80k | 40–180 total events |
| Farmer seasonal member | produce sales, seasonal lumps | cash deposit, wallet, branch, occasional remittance | deposits after harvest, loan draw before inputs/school fees | high rural cash intensity | long low-balance spells followed by harvest spikes | 6–24 off-season; 20–60 harvest months |
| Diaspora-supported household | remittance + local casual income | remittance to wallet/bank, wallet P2P, cash-out | modest savings if remittances are stable; school and medical spends dominate | moderate-high cash-out after inbound remittance | temporary balance spikes immediately after remittance | 8–35 ledger events; 15–80 total |
| Boda boda operator | daily rides, prior KSh 20k–80k net monthly | wallet P2P, till, cash, small SACCO savings | tiny frequent saves; emergency or bike-repair borrowing | very high cash-wallet cycling | end-day low balances, weekly restocking pattern | 60–220 total events |
| Church account | offerings, tithes, events | cash deposit, paybill/till, EFT/RTGS for suppliers, cheque-like bank flows | goal-based savings, minimal retail borrowing, occasional property/project loan | cash-heavy collection days | bursty balances around services/events | 12–70 events depending congregation size |
| Fraudulent shell member | fabricated or borrowed identity; low declared income | inbound wallet, branch cash, quick outward transfers, sometimes small initial check-off illusion | deposit boost, fake affordability, pass-through, short-lived activity | whatever maximises layering | brief spikes with near-complete cash-out/pass-through | 20–120 suspiciously concentrated events |

### Explicit flow maps

**Teacher in a SACCO.** Salary lands monthly through payroll, with automatic check-off splitting into deposits, loan recovery, and possibly share capital. Residual net pay lands in bank/FOSA, then leaks into M-Pesa for household transfers and merchant spending. Quarter-start school-fee pressure produces school-fee loans or intensified wallet spending. Good standing makes the first meaningful BOSA loan often come only after contribution history is established. citeturn33search2turn33search0turn16view4

**County worker.** Same basic template as the teacher, but simulate a higher probability of payroll timing inconsistency and the resulting behavioural traces: bridge borrowing, missed standing orders, end-of-month cash strain, and temporary arrears. This is a modelling prior informed by Kenya’s public-sector payroll architecture rather than a published county-specific delinquency series. citeturn52search0turn37search0

**SME owner using SACCO and M-Pesa.** Inbound receipts arrive via till/paybill, wallet P2P, and cash. The owner shifts float between wallet, business current/FOSA, and working-capital needs, with frequent merchant-like and household-like mixing. Biashara or asset-finance borrowing occurs on shorter business cycles than salary-backed members. Unaitas and police/Stima product menus justify this persona. citeturn32search0turn33search5turn33search8

**Farmer seasonal member.** Savings are lumpy, not smooth. Contribution velocity jumps after produce sale, then decays. Loan demand often leads income because inputs, school fees, and emergencies arrive before harvest cash. In the absence of a fully sourced county-crop calendar in this pass, seasonality should be parameterised by county and crop rather than hard-coded nationally. citeturn29search5turn16view2

**Diaspora-supported household.** A household receives one or more remittances monthly or quarterly, usually via wallet, bank, or MTO-linked channels. A large share of inbound value is withdrawn, redistributed to family members, or consumed quickly. Remittance receipts can temporarily inflate affordability signals before a SACCO loan application unless the simulator explicitly tags them as external support rather than stable employment income. citeturn34search2turn36view0turn36view1

**Boda boda operator.** This is a high-frequency, low-balance, mobile-first persona. Model many small inbound/outbound wallet movements, frequent airtime and fuel-like spends, weekend peaks, and limited formal documentation. The member may join a SACCO for a bike, emergency cover, or discipline, but the dominant economic texture is rapid wallet and cash turnover. FinAccess’s daily-mobile-money result is the key behavioural anchor here. citeturn16view1turn16view4

**Church account.** Public SACCO offerings explicitly include church accounts. Model clustered inflows after services/events, periodic supplier or welfare outflows, occasional project collections, and governance patterns that differ from individual accounts. Church accounts are especially important because they can look “bursty but explainable” under normal use and “bursty and abusive” under misuse. citeturn32search0

**Fraudulent shell member.** Base this persona on documented Kenyan typologies rather than invention: stolen or borrowed IDs, multiple mobile accounts, suspect funds from abroad or from mobile-till structures, short account age, weak economic history, and rapid dispersal or cash-out. The shell member should often have implausibly clean KYC on day zero and implausibly messy transactional behaviour shortly after. citeturn44view1turn45search2turn45search3

**HOW THIS FEEDS KENYA_SACCO_SIM:** create personas as state machines, not labels. Each persona needs an income generator, rail preference vector, saving policy, borrowing policy, cash-conversion propensity, and shock-response policy.

## AML attack surface and detection features

entity["organization","Financial Action Task Force","global aml body"] placed Kenya under increased monitoring in February 2024, and the jurisdiction remained on the increased-monitoring list in February 2026. entity["organization","Eastern and Southern Africa Anti-Money Laundering Group","regional aml body"]’s 2022 mutual-evaluation work is especially useful for simulation because it identifies the Kenyan vulnerabilities that matter operationally: cash-economy exposure, illicit inflow/outflow risk, trade-based schemes, mobile-money abuse, and corruption proceeds. citeturn39search8turn39search16turn44view0turn44view4turn44view2

SACCOs sit in a particularly awkward AML zone. They are member-owned, relationship-based, and often payroll-linked, which makes them powerful for inclusion but also creates laundering opportunities that are very different from those in commercial banks. The public Kenyan guidance architecture now explicitly pulls SACCOs into AML/CFT/CPF supervision; SASRA published specific AML/CFT/CPF guidance for regulated SACCOs in 2024, and Kenya’s POCAMLA regime requires suspicious/unusual transaction reporting within two days after suspicion arises, plus CTR filing for cash transactions at or above USD 15,000 equivalent. citeturn39search3turn39search11turn46search1turn46search3

The most important Kenyan-specific vulnerability to model is wallet-assisted layering. ESAAMLG notes that mobile wallets can be funded by third-party cash deposits and that such third parties are not necessarily identified. It also records case examples involving mobile-money accounts opened fraudulently with IDs belonging to unsuspecting individuals, funds received from South Africa, and till operators under company structures receiving suspect money. This is extremely relevant to SACCOs because many SACCOs now accept wallet-originated deposits into member accounts, making wallet-to-SACCO funnel behaviour a realistic laundering path. citeturn44view1

Corruption proceeds are another essential source class. ESAAMLG’s Kenya evaluation states that authorities viewed corruption proceeds as among the highest-value ML sources and cites high-impact anti-corruption cases valued above KSh 50.5 billion, including county-government embezzlement and procurement abuse. For a SACCO simulator, this means you should not restrict suspicious typologies to drug or cyber proceeds; procurement and public-funds diversion must be represented, especially in public-sector or county-linked SACCO populations. citeturn44view2

### Typology catalogue for injection

The table below is the recommended first-wave typology library for KENYA_SACCO_SIM. The mechanics are grounded in Kenyan AML sources, SACCO product design, and wallet-linked operating patterns.

| Typology | How it works in SACCO context | What transactions look like | High-value features |
|---|---|---|---|
| Structuring through branch / agent deposits | Illicit funds broken into sub-threshold cash or wallet-funded credits into FOSA/BOSA | repeated KSh-sized deposits, same-day or same-week, often rounded, across branch/agent/wallet | count of sub-threshold deposits, entropy of locations, post-deposit cash-out speed |
| Mule member account | Genuine or stolen identity used as transient receiver | weak income history, sudden inbound bursts, outward transfers to unrelated parties | account age, KYC-age mismatch, pass-through ratio, destination diversity |
| Rapid pass-through | Funds enter and exit with negligible retention | inflow followed within minutes/hours by wallet/bank/cash-out | inflow-outflow lag, retained-balance ratio, same-day turnover |
| Fake affordability before loan | Temporary deposits/remittances used to inflate loan capacity | short pre-application balance build, deposit boost, immediate application | 30-day balance slope before application, external-funding share, guarantor density |
| Payroll proxy abuse | Fraudster channels funds through account made to resemble salary or check-off | recurring “salary-like” credits without stable employer metadata | employer consistency score, salary narration similarity, calendar periodicity mismatch |
| Multiple accounts linked to one phone/device | Same MSISDN/device reused across members or beneficiaries | shared phone, device, app install, geo pattern | phone-to-member degree, device graph centrality |
| Wallet funnel to SACCO | Many wallets pay into one SACCO account or vice versa | many paybill refs to one target, followed by dispersion | unique payer count, payer-to-beneficiary graph expansion, paybill burstiness |
| Procurement-corruption parking | Proceeds of inflated tenders parked in employee/church/community accounts | large unusual credits tied to public-sector counterparties, then staged exits | public-entity counterparty flag, unusual amount cluster, beneficiary network distance |
| Church / charity misuse | Legitimate donation-looking inflows used as cover | periodic collections plus unrelated high-value transfers | day-of-week concentration, donor-base stability, project narrative mismatch |
| Dormant-account reactivation | Old member account revived for laundering or fraud | long dormancy, PIN reset/KYC refresh, then large activity burst | days dormant, first-credit size, first-week velocity |
| Remittance layering | foreign-source funds arrive, then are broken and redistributed | remittance inward, multiple wallet/bank/cash outputs | corridor flag, remittance-to-local-transfer fanout, beneficiary overlap |
| Insider-guarantor fraud ring | employees or ring members guarantee one another / override controls | dense guarantor networks, reused guarantors, linked defaults | guarantor graph motifs, guarantor concentration, default contagion |
| Till/paybill shell activity | company/till or paybill reference used to cycle suspicious funds | merchant-like credits without matching trade profile | merchant-category mismatch, low settlement retention, no genuine retail seasonality |

Kenya-specific indicator design should therefore go beyond static thresholds. Use behavioural and graph features:

- **Velocity:** minutes/hours from credit to debit.
- **Retention:** end-of-day and end-of-week retained percentage.
- **Purpose consistency:** salary narration, employer ID, PRN/reference plausibility.
- **Identity reuse:** shared phone, device, address, introducer, guarantor.
- **Trajectory:** deposit build-up in the 30–90 days before a loan.
- **Source mix:** payroll vs remittance vs wallet vs cash.
- **Counterparty graph:** fan-in, fan-out, closed rings, bridge nodes.
- **Channel shift:** dormant branch-only member suddenly going wallet-heavy at night.
- **Economic coherence:** declared job type versus transaction density and amount profile. citeturn45search0turn45search2turn45search3turn45search9turn44view1

### Alert logic that should exist on day one

```text
IF member.cash_deposit_count_7d >= N
AND max_single_cash_deposit < ctr_threshold_local
AND total_cash_deposit_7d > structuring_limit
THEN alert = STRUCTURED_DEPOSITS

IF inbound_value_24h > X
AND outbound_value_24h / inbound_value_24h > 0.85
AND retained_balance_eod < Y
THEN alert = RAPID_PASS_THROUGH

IF loan_application_within_30d = true
AND external_non_salary_credits_share_30d > A
AND balance_growth_30d > B
THEN alert = AFFORDABILITY_WINDOW_DRESSING

IF guarantor_graph_cycle_len <= 4
AND reused_guarantor_count > C
AND delinquency_cluster_ratio > D
THEN alert = GUARANTOR_RING
```

Under Kenyan law and guidance, a realistic supervisor environment should also enforce reporting clocks: suspicious activity/transactions should be reportable within two days after suspicion arises, while CTR logic should exist for cash transactions at or above the prescribed threshold. citeturn46search1turn46search3

**HOW THIS FEEDS KENYA_SACCO_SIM:** typologies should be injected as **behavioural overlays** on otherwise normal personas. Do not generate “criminal accounts” from scratch only; create members whose baseline resembles genuine Kenyan behaviour, then perturb the rail mix, timing, balance retention, graph links, and loan interactions.

## Calibration priors and temporal engine

The strongest hard calibration anchors available publicly are: a very large mobile-money user base and transaction value; daily wallet dominance; a public-sector earnings mean of KSh 933.1 thousand per year in 2024; 6.84 million regulated SACCO members in 2023 with KSh 758.57 billion gross loans and KSh 682.19 billion deposits; and remittances of USD 4.945 billion in 2024. Everything else at the micro level should be built as priors around those anchors, then validated. citeturn52search0turn2view0turn16view1turn29search5turn34search2

### Recommended first-pass quantitative priors

The table below is intentionally explicit about what is observed versus inferred.

| Variable | Recommended prior for v1 | Status |
|---|---|---|
| Formal salaried monthly gross inflow | centre around KSh 78k; teacher prior KSh 45k–110k; county/public worker KSh 35k–95k | anchored to KNBS public-sector mean; persona bands inferred |
| BOSA monthly check-off contribution | 5%–20% of gross salary depending product mix and loan load | inferred from SACCO mechanics |
| BOSA development-loan size | 2.0x–4.0x member deposits; common cap default 3.0x | anchored to public SACCO FAQ; upper end inferred |
| FOSA instant advance | KSh 2k–80k, short tenor | inferred from public product menus |
| Cash / wallet retail deposit size | KSh 200–20k common retail band; heavier tails for SMEs/churches | inferred |
| Monthly total transaction count, salaried member | 25–70 total financial touches; 12–30 SACCO-ledger events | inferred from daily-wallet dominance + monthly obligations |
| Monthly total transaction count, SME/boda | 60–220 total events | inferred |
| Remittance receipt | KSh 5k–80k per episode common household band; quarterly and monthly mixtures | inferred from 2024 aggregate remittances + survey channel evidence |
| Dormancy | begin with 5%–15% dormant-account share institution-wide | modelling prior; needs institution data |
| Annual default / material delinquency hazard | 3%–8% stable payroll cohorts; 8%–18% volatile or seasonal cohorts | modelling prior; needs PAR data |
| Fraud prevalence among members | 0.3%–1.5% with event-level suspicious concentration much higher than member-level prevalence | modelling prior |
| Channel mix for adults | wallet dominant, bank secondary, SACCO tertiary but monthly-important | anchored to FinAccess |

Two tactical rules will make the synthetic dataset feel much more real:

1. **Separate “financial touches” from “SACCO ledger events.”** A teacher can perform 40 wallet/till transfers in a month but generate only 15 SACCO-booked entries.
2. **Split stable inflow from spendable inflow.** Gross salary, check-off-available salary, and free-cash balance are not the same object. citeturn16view4turn33search2

### Temporal effects to simulate

**Payday spikes.** Formal payroll cohorts should show strong month-end or employer-cycle periodicity. The dominant pattern is: salary/check-off credit, loan recovery, deposit posting, then immediate wallet leakage over the next 1–5 days. Public-sector workers should be the cleanest periodic cohort. citeturn52search0turn33search2

**School-fee spikes.** This is a defining Kenyan SACCO behaviour. Publicly advertised school-fee loans at Mwalimu and other SACCOs mean the simulator should impose term-start stress when households need lump-sum education payments. If exact term dates are not externally attached, expose a configurable `school_term_start_months` parameter instead of hard-coding. citeturn33search0turn33search3

**Holiday spikes.** December should raise merchant-like spend, remittance receipts, and cash-out pressure. CBK data already shows strong remittance resilience and December inflow strength in 2024. citeturn34search2

**Weekend wallet usage.** FinAccess’s daily mobile-money intensity implies that weekends should not look quiet in wallet-heavy cohorts. Increase P2P, merchant/till, and airtime/utilities on Fridays through Sundays, especially for informal earners and urban households. citeturn16view1turn16view4

**Month-end loan pressure.** Members near salary date should show higher bridge borrowing, failed standing orders, and tighter free balances if already highly leveraged. This is especially important for county-worker or distressed teacher variants. citeturn33search2turn49view1

**Rural–urban differences.** Rural SACCO members still show stronger traditional-channel preference, while urban members are more digitally confident. Reflect that in branch versus wallet versus app choice probabilities, and in cash intensity. citeturn16view2

**Night anomalies.** Public sources in this pass do not provide a clean hour-of-day distribution for SACCO or wallet traffic. Treat late-night surges as an anomaly model rather than a normal baseline pattern: genuine retail activity can occur at night, but high-value branch-like or payroll-like behaviour at unusual hours should be rare. This is a modelling decision, not a published frequency fact. citeturn16view1turn44view1

### Implementation logic for seasonality

```text
balance_t = balance_{t-1}
          + income_events(t, persona, calendar)
          + remittance_events(t, corridor_mix, seasonality)
          - mandatory_deductions(t, payroll_rules, loans)
          - household_spend(t, day_type, school_cycle, holiday_cycle)
          - fraud_overlay_outflows(t, typology_state)

if month in school_term_start_months:
    increase(school_fees_spend, school_fees_loan_demand)

if day is salary_window:
    increase(wallet_cashout, household_transfers, merchant_spend)
    decrease(free_balance_retention)

if persona in seasonal_agri and month in harvest_months[county,crop]:
    increase(produce_sale_credits, deposit_topups)
else:
    increase(bridge_borrowing_probability)
```

**HOW THIS FEEDS KENYA_SACCO_SIM:** calibrate the simulator around a few strong public anchors, then let the temporal engine create realistic clustering. Without time clustering, even good per-transaction distributions will still look synthetic.

## Data model and simulation architecture

The simulator should produce the six requested core files plus four support tables. The schema below is opinionated toward AML engineering rather than consumer analytics.

### Core tables

| Table | Key fields | Why it matters |
|---|---|---|
| `members.csv` | `member_id, institution_id, persona_type, county, subcounty, urban_rural, occupation, employer_id, join_date, kyc_level, risk_segment, phone_hash, alt_phone_hash, id_hash, introducer_member_id, referral_source, dormant_flag` | member-level AML, linkage, and lifecycle |
| `accounts.csv` | `account_id, member_id, acct_type(BOSA_DEPOSIT/FOSA_PRIME/SHARE_CAPITAL/CHURCH/SCHOOL/BUSINESS), product_code, open_date, status, linked_msisdn_hash, paybill_ref, branch_id, current_balance, available_balance` | one member may hold several economic pockets |
| `loans.csv` | `loan_id, member_id, product_code, application_date, approval_date, disbursement_date, principal, tenor_months, rate_type, repayment_mode, disbursement_channel, performing_status, arrears_days, restructure_flag, purpose_code` | loan realism and AML-affordability analysis |
| `guarantors.csv` | `loan_id, guarantor_member_id, guarantee_amount, guarantee_pct, pledge_date, release_date, guarantor_deposit_balance_at_pledge, relationship_type` | SACCO-specific fraud graph |
| `transactions.csv` | `txn_id, ts, institution_id, account_id_dr, account_id_cr, member_id_primary, rail, channel, provider, counterparty_type, counterparty_id_hash, amount_kes, fee_kes, currency, narrative, reference, branch_id, agent_id, device_id, geo_bucket, batch_id, is_reversal` | main event log |
| `alerts_truth.csv` | `alert_id, txn_id_or_entity_id, alert_type, injected_typology, severity, start_ts, end_ts, truth_label, explanation_code` | supervised AML evaluation |

### Support tables

| Table | Key fields | Why it matters |
|---|---|---|
| `devices.csv` | `device_id, member_id, first_seen, last_seen, os_family, app_user_flag, shared_device_group` | mule and linkage detection |
| `employers.csv` | `employer_id, employer_type, payroll_frequency, sector, public_private, county_code, checkoff_supported` | salary periodicity and payroll plausibility |
| `branches.csv` | `branch_id, institution_id, county, urban_rural, branch_type(hq/branch/agent_desk), opening_date` | geography and traditional-channel behaviour |
| `agents.csv` | `agent_id, provider, county, location_type, active_from, active_to` | cash-in/cash-out locality and clustering |

### Recommended transaction taxonomy

Use a fixed controlled vocabulary. At minimum:

`SALARY_IN`, `CHECKOFF_DEPOSIT`, `CHECKOFF_LOAN_RECOVERY`, `SHARE_CAPITAL_TOPUP`, `BOSA_DEP_TOPUP`, `FOSA_CASH_DEPOSIT`, `MPESA_PAYBILL_IN`, `PESALINK_IN`, `PESALINK_OUT`, `WALLET_P2P_IN`, `WALLET_P2P_OUT`, `AGENT_CASHIN`, `AGENT_CASHOUT`, `LOAN_DISBURSEMENT_BOSA`, `LOAN_DISBURSEMENT_FOSA`, `LOAN_REPAYMENT`, `INTEREST_ACCRUAL`, `PENALTY_POST`, `DIVIDEND_POST`, `REBATE_POST`, `ACCOUNT_REACTIVATION`, `KYC_REFRESH`, `RTGS_OUT`, `EFT_BATCH_IN`, `EFT_BATCH_OUT`, `GOV_PAYMENT_IN`, `TAX_PAYMENT_OUT`, `CHURCH_COLLECTION_IN`, `BUSINESS_SETTLEMENT_IN`, `REVERSAL`.

### Seven-layer generator

| Layer | Responsibility | Output |
|---|---|---|
| Population generator | create members, institutions, occupations, counties, phones, employers | baseline entities |
| Normal behaviour engine | rail preferences, balances, spend, transfers, deposit habits | clean transactions |
| Calendar / seasonality engine | payday, school fees, holidays, harvest, shocks | time modulation |
| Product engine | onboarding, contributions, loans, guarantors, arrears, dividends | SACCO semantics |
| Fraud / AML injector | apply typology overlays to selected members or networks | suspicious traces |
| Ground-truth labeler | write member-, cluster-, and txn-level truth labels | evaluation labels |
| Export engine | snapshot CSVs, train/val/test splits, metadata manifest | final dataset |

### Minimal pseudocode

```python
for institution in institutions:
    members = generate_members(institution.profile)

for day in simulation_calendar:
    for member in active_members(day):
        emit_income_events(member, day)
        emit_household_spend(member, day)
        emit_sacco_contributions(member, day)
        emit_loan_events(member, day)
        apply_cash_wallet_conversions(member, day)

for typology in injected_typologies:
    overlay_transactions(typology.targets, typology.rules, calendar)

derive_balances()
derive_arrears()
label_truth()
export_csvs()
```

The schema should also encode compliance-relevant metadata even if the source institution would not normally expose all of it in a customer statement. For example, keep separate fields for `channel`, `rail`, `provider`, `counterparty_type`, `counterparty_hash`, `branch_id`, `agent_id`, and `device_id`. That field separation is what enables later rule testing. citeturn46search1turn46search3

**HOW THIS FEEDS KENYA_SACCO_SIM:** the schema should be built for *explainable anomalies*. If an alert fires, the dataset must let you reconstruct rail, device, counterparty class, employer linkage, guarantor network, and time pattern.

## Validation, build plan, and open questions

A simulator is not realistic because its stories sound plausible. It is realistic when measurable ratios match the external environment and when injected typologies remain hard to distinguish from normal activity except through the intended features.

### Validation scorecard

Start with these measurable checks:

| Metric | Validation target |
|---|---|
| Adults using wallet-like channels | dominant channel in aggregate, consistent with FinAccess 82.3% usage |
| Daily wallet activity share | materially higher than daily bank activity, consistent with FinAccess 52.6% vs 4.8% |
| SACCO monthly-engagement pattern | SACCO events concentrated around monthly obligations rather than daily retail |
| Institution concentration | top decile of institutions should control a disproportionate share of deposits/assets |
| Salary periodicity | formal cohorts exhibit strong monthly autocorrelation and employer-linked narration stability |
| Rural vs urban channel mix | rural members more branch/traditional; urban members more digital |
| Deposit-multiple borrowing | long-term BOSA loans cluster around deposit-linked multiples |
| Pass-through base rate | low in clean population, high in injected rapid-pass-through typologies |
| Dormant reactivation signature | dormant accounts should rarely wake with immediately extreme throughput unless typology injected |
| CTR coverage | all synthetic cash events above threshold produce reportable traces |
| Typology prevalence | suspicious members should be rare, suspicious transactions less rare, suspicious subgraphs rarer but higher impact |

A useful numeric acceptance gate for v1 is:

- formal salaried cohorts: salary day autocorrelation above 0.75,
- rapid-pass-through false generation below 3 percent of clean members,
- wallet/share of total event count higher than any other rail,
- rural members at least 1.2x more likely than urban members to use branch/traditional SACCO interactions,
- top 15 percent of institutions holding more than half the simulated assets/deposits. citeturn16view2turn16view4turn29search5

### Thirty-day build plan

**Week one.** Build the population generator, institution archetypes, accounts schema, baseline rails, and a clean transaction generator for salary, wallet P2P, paybill, cash-in/out, SACCO contributions, and simple loan disbursement. Export `members`, `accounts`, and `transactions` first. Use only clean behaviour and a tiny number of institution archetypes. citeturn29search5turn16view3turn2view0

**Week two.** Add SACCO realism: BOSA/FOSA split, share capital, check-off, deposit multiples, guarantors, employer linkage, salary sweep, arrears, refinance, school-fee loan seasonality, and exit/dormancy logic. Export `loans` and `guarantors` with coherent internal state transitions. citeturn33search2turn33search0turn30search0

**Week three.** Add AML overlay library: structured deposits, pass-through, affordability window dressing, remittance layering, dormant-reactivation abuse, multiple-account/device linkage, and guarantor-ring fraud. Implement truth labeling at transaction, member, and cluster level. Export `alerts_truth`. citeturn44view1turn45search2turn45search3turn46search3

**Week four.** Freeze validation benchmarks, produce reproducible seeds, generate benchmark splits, and document parameter files. Include a baseline rule pack and a benchmark notebook that reports channel mix, salary periodicity, deposit-multiple distributions, pass-through metrics, and alert hit rates. citeturn16view1turn29search5turn34search2

### Open questions and limitations

This research pass surfaced several gaps that matter for a production-grade v2:

- I did **not** recover a clean, machine-readable, official microdistribution for 2024 SACCO PAR/default by SACCO type; default priors above are therefore modelling assumptions.
- I did **not** assemble county-by-county crop calendars from primary sources in this pass; harvest timing should remain a configurable input.
- Public sources confirm mobile-money scale and agent count, but not a full public split of behaviour by provider, so provider market shares should not be overfit.
- The 2024 SASRA annual report is publicly announced, but some detailed 2024 institution metrics were not cleanly retrievable in this pass through accessible official snippets; the 2023 official report is therefore the stronger hard baseline in this dossier. citeturn24view1turn29search5

**HOW THIS FEEDS KENYA_SACCO_SIM:** ship v1 with explicit priors and validation gates, then replace priors incrementally with institution-specific evidence. The simulator should be designed so that better Kenyan evidence can swap into parameter files without changing the core engine.