import { Author, Comment, Post } from './exampleSchema.fixture';
import { DataMapper, DynamoDbSchema } from '@aws/dynamodb-data-mapper';
import { isSchema } from '@aws/dynamodb-data-marshaller';

jest.mock('uuid', () => ({v4: jest.fn(() => 'uuid')}));

describe('annotations', () => {
    it('should create a schema that includes references to property schemas', () => {
        const postSchema = (Post.prototype as any)[DynamoDbSchema];
        expect(isSchema(postSchema)).toBe(true);
        expect(isSchema(postSchema.author.members)).toBe(true);
        expect(isSchema(postSchema.replies.memberType.members)).toBe(true);
    });

    it('should support recursive shapes in the generated schema', () => {
        const commentSchema = (Comment.prototype as any)[DynamoDbSchema];
        expect(isSchema(commentSchema)).toBe(true);
        expect(isSchema(commentSchema.replies.memberType.members)).toBe(true);
        expect(commentSchema.replies.memberType.members).toBe(commentSchema);
    });

    it('should marshall a full object graph according to the schema', async () => {
        const promiseFunc = jest.fn(() => Promise.resolve({Item: {}}));
        const mockDynamoDbClient = {
            config: {},
            putItem: jest.fn(() => ({promise: promiseFunc})),
        };

        const mapper = new DataMapper({
            client: mockDynamoDbClient as any,
        });

        const post = new Post();
        post.createdAt = new Date(0);
        post.author = new Author();
        post.author.name = 'John Smith';
        post.author.photo = Uint8Array.from([0xde, 0xad, 0xbe, 0xef]);
        post.author.socialMediaHandles = new Map([
            ['github', 'john_smith_27834231'],
            ['twitter', 'theRealJohnSmith'],
        ]);
        post.title = 'Review of Rob Loblaw\'s Law Blog';
        post.subtitle = 'Does it live up to the hype?';
        post.content = "It's a great law blog.";
        post.corrections = [
            'The first edition of this post did not adequately attest to the law blog\'s greatness.'
        ];
        post.replies = [new Comment()];

        post.replies[0].author = 'Rob Loblaw';
        post.replies[0].timestamp = new Date(0);
        post.replies[0].subject = 'Great review';
        post.replies[0].text = 'Appreciate the congrats';
        post.replies[0].upvotes = 35;
        post.replies[0].downvotes = 0;
        post.replies[0].approved = true;

        const reply = new Comment();
        reply.author = 'John Smith';
        reply.timestamp = new Date(60000);
        reply.subject = 'Great review of my review';
        reply.text = 'Thanks for reading!';
        reply.approved = true;

        post.replies[0].replies = [reply];

        await mapper.put(post);

        expect((mockDynamoDbClient.putItem.mock.calls[0] as any)[0])
            .toMatchObject({
                ConditionExpression: 'attribute_not_exists(#attr0)',
                ExpressionAttributeNames: {'#attr0': 'version'},
                TableName: 'Posts',
                Item: {
                    author: {M: {
                        name: {S: "John Smith"},
                        photo: {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])},
                        socialMediaHandles: {M: {
                            github: {S: "john_smith_27834231"},
                            twitter: {S: "theRealJohnSmith"}
                        }}
                    }},
                    content: {S: "It's a great law blog."},
                    corrections: {L: [
                        {S: "The first edition of this post did not adequately attest to the law blog's greatness."}
                    ]},
                    createdAt: {N: "0"},
                    id: {S: "uuid"},
                    replies: {L: [
                        {M: {
                            approved: {BOOL: true},
                            author: {S: "Rob Loblaw"},
                            downvotes: {N: "0"},
                            replies: {L: [
                                {M: {
                                    approved: {BOOL: true},
                                    author: {S: "John Smith"},
                                    subject: {S: "Great review of my review"},
                                    text: {S: "Thanks for reading!"},
                                    timestamp: {N: "60"}
                                }}
                            ]},
                            subject: {S: "Great review"},
                            text: {S: "Appreciate the congrats"},
                            timestamp: {N: "0"},
                            upvotes: {N: "35"}
                        }}
                    ]},
                    subtitle: {S: "Does it live up to the hype?"},
                    title: {S: "Review of Rob Loblaw's Law Blog"},
                    version: {N: "0"}
                },
            });
    });
});
