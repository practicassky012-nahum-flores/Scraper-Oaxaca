# Legal Copilot 
**Copyright © 2025 Sky Social Impact, S.A.P.I. All rights reserved.**

# SCRAPING - Legal Copilot API

## Project Overview
Scraping-based for document, processing extraction and analysis.

## Tech Stack
- **Framework**: Selenium
- **Database**: SQLAlchemy with Alembic migrations
- **Cloud Services**: Google APIs
- **Document Processing**: python-docx, reportlab, lxml

## Project Structure
```
leyes/
├── 
├── 
├── 
├── 
├── 
├── 
└──
```

## Key Commands
- **Run server**: `uvicorn app.main:app --reload`

## Development Notes
- Main API entry point: `app/main.py`
- Database models in `app/models/`
- API routes in `app/router/` (sic_router.py, admin_router.py)
- Services for business logic in `app/services/`
- **Security**: API Key authentication via X-API-Key header
- Uses custom documentation URLs for security
- CORS configured for Azure deployment

## Security


## Environment Setup
- Requires credentials in `gcredential.json` for Google services



In this repo will use as a container to develop the frontend.

To get started, we’ll need to install `Git Bash`.

**Daily workflow bellow:**



## Required Rules:
- Do **not** code direct on `main`
- Each developer must create their `own branch`
- Perfomr a `daily push` in order to back up the repository.
- Once your code is ready, you may merge it into `main`.


## Workflow Steps:

*If there have been updates to the main branch:*

1.  Switch to `main` and pull the latest changes:

        git checkout main

        git pull origin main

2. Switch to your own branch:
        
        git checkout <your-branch-name>

3. Rebase or merge the latest changes from `main`
        
        git rebase main (optional)

        Or, if you want to avoid rebasing, use merge instead:
        
        git merge main

4. Work on your own branch:
        
        - Make your changes, 
        
        - Test your code locally until it works.

5. Save (back-up) and upload (push) your modifications:

        git add .  
        
        git commit -m "message to deploy" 
        
        git push origin <your-branch-name>

6. Switch to `main`:

        git checkout main

7. Merge your own branch into `main`:

        git merge <your-branch-name>

8. Push the updated `main` branch to GitHub:

        git push origin main