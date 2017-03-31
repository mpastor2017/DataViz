# Instructions for Homework Assignment Submission

Please follow the instructions outlined below when submitting your homework assignments. **Failure 
to follow the instructions will result in lost points!**

### Step 1 (Setup)
For each homework assignment you will have to create a new git (local) repository on your laptop/wolf account. The command to do this is:

```
git init
```
You can add/commit/etc files to your local repository, but **DO NOT** add/commit any directories.  

### Step 2 (Creating a branch)
Once you are ready to submit your solution, you must first create a branch on your local repository. To do this, you must have committed at least once to your local repository, so it makes sense to perform this step after you are done with the assignment. If you try to create a branch without first committing, you will get this error: *fatal: You are on a branch yet to be born*.

The command to create a branch is:

```
git checkout -b <branch_name>
```

The \<branch_name\> has to be your name **exactly as it appears on Canvas** with underscores instead of spaces. For example, if I were to create a branch I would do: 

```
git checkout -b Alexandros_Nathan
```

### Step 3 (Link local repo to remote)
Now that you have created your branch, you must specify where you will be pushing your branch. This is done as follows:

```
git remote add origin git@github.com:MSIA/bigdatacourse.git
```

### Step 3 (Push to remote repo)
Now you are ready push your solutions:
```
git push -u origin <branch_name>
```
If this step is completed successfully, you will see the following message: *Branch <\branch_name\> set up to track remote branch <\branch_name\> from origin*.


### Note:
If you upload your solutions a long time before the deadline, other students will have access to them. It would be best if you pushed your work to the remote repository as close to the deadline as possible. 

