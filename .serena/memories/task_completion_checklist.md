# Task Completion Checklist

## Before Committing Changes

### Code Quality
- [ ] **Follow naming conventions**: snake_case for functions/variables, PascalCase for classes
- [ ] **Add appropriate logging**: Use logger.info(), logger.error(), logger.warning()
- [ ] **Handle errors gracefully**: Try/catch blocks with meaningful error messages
- [ ] **Update docstrings**: Document any new functions or significant changes
- [ ] **Check imports**: Organize imports (standard → third-party → local)

### Testing
- [ ] **Run the pipeline manually**: `python scripts/run_pipeline.py`
- [ ] **Verify data collection**: Check that CSV files are created in `data/`
- [ ] **Test upload process**: Ensure Dune tables are updated correctly
- [ ] **Check logs**: Review log files for errors or warnings
- [ ] **Validate data integrity**: Verify row counts and column structure

### Environment & Security
- [ ] **Protect sensitive data**: Ensure no API keys in code or commits
- [ ] **Update .env.example**: Add any new required environment variables
- [ ] **Check .gitignore**: Verify sensitive files are excluded
- [ ] **Test with environment variables**: Ensure pipeline works with secrets

### Documentation
- [ ] **Update README.md**: Document any new features or changes
- [ ] **Add inline comments**: Explain complex logic or business rules
- [ ] **Update schema definitions**: If CSV structure changes
- [ ] **Document API changes**: Note any endpoint or parameter updates

## Git Workflow
```bash
# 1. Check current status
git status

# 2. Stage changes
git add .

# 3. Commit with descriptive message
git commit -m "Description: What was changed and why

- Specific change 1
- Specific change 2
- Any breaking changes or important notes

🤖 Generated with Claude Code

Co-Authored-By: Claude <noreply@anthropic.com>"

# 4. Push to GitHub
git push origin main
```

## Deployment Verification
- [ ] **GitHub Actions pass**: Check workflow status after push
- [ ] **Manual trigger test**: Use workflow_dispatch to test manually
- [ ] **Monitor first automated run**: Verify scheduled execution works
- [ ] **Check Dune tables**: Confirm data appears correctly in Dune Analytics
- [ ] **Verify no duplicates**: Query tables to ensure clear-and-replace works

## Emergency Rollback
If deployment causes issues:
```bash
# Revert to last working commit
git log --oneline -10  # Find last good commit
git reset --hard <commit-hash>
git push origin main --force-with-lease

# Or disable GitHub Actions temporarily
# Go to GitHub → Actions → Disable workflow
```

## Success Criteria
✅ **Pipeline runs successfully**
✅ **Data appears in Dune tables**
✅ **No duplicate data**
✅ **Logs show no errors**
✅ **GitHub Actions pass**
✅ **Manual testing successful**