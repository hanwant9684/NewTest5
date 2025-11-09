# IMPORTANT: Legal Documents Setup Instructions

## ⚠️ ACTION REQUIRED

Before deploying your bot to production, you **MUST** fill in your personal/business information in the legal documents. The current documents contain placeholders marked with `[BRACKETS]` that need to be replaced with your actual information.

## Required Information

### 1. Service Provider Information
You need to provide:
- **Your legal name or business name**
- **Complete address** (Street, City, State, PIN Code, India)
- **Contact email** (a professional email you check regularly)
- **Telegram username** (your bot support username)
- **City for jurisdiction** (the city whose courts will have jurisdiction)

### 2. Grievance Officer Details (Mandatory under India IT Act 2000)
The Grievance Officer handles user complaints. This can be you or a designated person.

Required details:
- **Name** of the grievance officer
- **Email** address for complaints
- **Designation** (e.g., "Owner", "Compliance Officer")
- **Phone number** (optional but recommended)
- **Address** for written complaints

**Legal Requirement:** Must respond within 24 hours and resolve within 15 days.

### 3. Data Protection Officer (For GDPR Compliance)
Required if you have EU users. This can be the same person as Grievance Officer.

Required details:
- **Name** of DPO
- **Email** address for data protection queries

### 4. Contact Emails
Set up these email addresses:
- **General contact email** (for user queries)
- **Privacy email** (for privacy-related requests)
- **DMCA/Copyright email** (for copyright complaints)
- **Grievance officer email** (for complaints)
- **DPO email** (for GDPR requests)

**Tip:** You can use the same email for all if you're a solo operator. Just make sure you check it regularly!

## Files to Update

### File 1: `legal/terms_and_conditions.txt`

Search for these placeholders and replace:
```
[YOUR LEGAL NAME OR BUSINESS NAME]
[YOUR COMPLETE ADDRESS, CITY, STATE, PIN CODE, INDIA]
[YOUR CONTACT EMAIL]
[YOUR TELEGRAM USERNAME]
[GRIEVANCE OFFICER NAME]
[GRIEVANCE OFFICER EMAIL]
[DPO NAME]
[DPO EMAIL]
[YOUR CITY]
[YOUR DMCA EMAIL]
```

**Location:** Section 16 "SERVICE PROVIDER INFORMATION"

### File 2: `legal/privacy_policy.txt`

Search for these placeholders and replace:
```
[YOUR LEGAL NAME OR BUSINESS NAME]
[YOUR COMPLETE ADDRESS, CITY, STATE, PIN CODE, INDIA]
[YOUR CONTACT EMAIL]
[YOUR TELEGRAM USERNAME]
[DPO NAME]
[DPO EMAIL]
[GRIEVANCE OFFICER NAME]
[DESIGNATION]
[GRIEVANCE OFFICER EMAIL]
[OPTIONAL PHONE NUMBER]
[COMPLETE ADDRESS FOR WRITTEN COMPLAINTS]
[YOUR PRIVACY EMAIL]
```

**Location:** Section 17 "SERVICE PROVIDER AND CONTACT INFORMATION"

## Quick Example

### Before:
```
Name: [YOUR LEGAL NAME OR BUSINESS NAME]
Email: [YOUR CONTACT EMAIL]
```

### After:
```
Name: Rajesh Kumar
Email: support@mybot.com
```

## Why This Matters

### Legal Requirements
1. **India IT Act 2000 & Intermediary Rules:**
   - Requires service providers to publish contact details
   - Must designate a Grievance Officer
   - Must respond to complaints within specified timelines

2. **GDPR (for EU users):**
   - Requires identification of Data Controller
   - Must provide Data Protection Officer contact
   - Users must know how to exercise their rights

### Your Protection
Without proper contact information:
- The legal documents **won't protect you** from liability
- You may face penalties under Indian law
- GDPR violations can result in heavy fines
- Users can claim the service is operating illegally

## After Updating

1. **Verify all placeholders are replaced** (search for `[` in both files)
2. **Double-check email addresses** work and you have access
3. **Test email delivery** to ensure you receive user complaints
4. **Restart your bot** to activate the changes
5. **Existing users** may need to re-accept the updated terms

## Legal Compliance Checklist

- [ ] Replaced all `[PLACEHOLDERS]` in terms_and_conditions.txt
- [ ] Replaced all `[PLACEHOLDERS]` in privacy_policy.txt
- [ ] Verified all email addresses are working
- [ ] Set up Grievance Officer (can be yourself)
- [ ] Set up Data Protection Officer (can be yourself)
- [ ] Ready to respond to complaints within 24 hours
- [ ] Understood the 15-day resolution requirement
- [ ] Know your local jurisdiction/courts

## Need Help?

If you're unsure about any of this:
1. **For legal advice:** Consult with a lawyer familiar with Indian IT law
2. **For simple setups:** Use the same email for all purposes and designate yourself as both Grievance Officer and DPO
3. **For business operations:** Consider getting professional legal review of the documents

## Example Filled Information (Solo Operator)

```
Service Provider: Rajesh Kumar
Address: 123 MG Road, Bangalore, Karnataka, 560001, India
Email: rajesh@example.com
Telegram: @RajeshSupport

Grievance Officer: Rajesh Kumar
Designation: Owner and Compliance Officer
Email: complaints@example.com
Phone: +91-9876543210
Address: 123 MG Road, Bangalore, Karnataka, 560001, India

Data Protection Officer: Rajesh Kumar
Email: privacy@example.com

Jurisdiction: Bangalore, India
```

---

**Remember:** Operating without proper legal compliance can expose you to liability. Take the time to fill this out correctly!
