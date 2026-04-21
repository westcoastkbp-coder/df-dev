from github import Github
import os


def main():
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("NO GITHUB TOKEN")
        return

    g = Github(token)
    repo = g.get_repo("westcoastkbp-coder/jarvis-digital-foreman")

    issue = repo.create_issue(
        title="DF SYSTEM TEST ISSUE 001",
        body="Created by github_issue_agent.py from Digital Foreman control system."
    )

    print(f"ISSUE_CREATED: #{issue.number}")
    print(issue.html_url)


if __name__ == "__main__":
    main()
