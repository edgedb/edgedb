module.exports = async ({ github, context }) => {
  const { VERCEL_TOKEN, VERCEL_TEAM_ID, GITHUB_HEAD_REF } = process.env;

  console.log(
    await github.rest.issues.listComments({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.issue.number,
    })
  );
  return;

  const res = await fetch(
    `https://api.vercel.com/v13/deployments?teamId=${VERCEL_TEAM_ID}`,
    {
      body: JSON.stringify({
        name: "edgedb-docs",
        gitSource: {
          type: "github",
          org: "edgedb",
          repo: "edgedb.com",
          ref: "docs-preview",
        },
        projectSettings: {
          buildCommand: `EDGEDB_REPO_BRANCH=${GITHUB_HEAD_REF} yarn vercel-build`,
        },
      }),
      headers: {
        Authorization: `Bearer ${VERCEL_TOKEN}`,
        "Content-Type": "application/json",
      },
      method: "post",
    }
  );

  if (res.ok) {
    const deployment = await res.json();
    console.log(deployment.url);

    github.rest.issues.createComment({
      issue_number: context.issue.number,
      owner: context.repo.owner,
      repo: context.repo.repo,
      body: `Deployed docs preview: <https://${deployment.url}>`,
    });
  } else {
    throw new Error(
      `vercel create deployment api request failed: ${res.status} ${res.statusText}`
    );
  }
};
